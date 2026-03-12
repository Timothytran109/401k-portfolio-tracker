import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
import os
from datetime import datetime, date

# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    # Same as Bronze — reads your pipeline_config.yaml into a Python dictionary
    with open("pipeline_config.yaml", "r") as f:
        return yaml.safe_load(f)

# ── Watermark ─────────────────────────────────────────────────────────────────
def load_watermark(silver_path: str):
    # The watermark is the most recent trade_date already in Silver
    # If Silver has never run before, there are no files and we return None
    # which tells load_new_bronze_records() to pull everything from Bronze
    parquet_files = [f for f in os.listdir(silver_path) if f.endswith(".parquet")] \
        if os.path.exists(silver_path) else []

    if not parquet_files:
        print("[Silver] No watermark found — first run, will process all Bronze records")
        return None

    # If Silver files exist, query them with DuckDB to find the latest date
    conn = duckdb.connect()
    result = conn.execute(f"SELECT MAX(trade_date) FROM '{silver_path}/*.parquet'").fetchone()
    watermark = result[0]
    print(f"[Silver] Watermark found — last processed date: {watermark}")
    return watermark

# ── Load New Bronze Records ───────────────────────────────────────────────────
def load_new_bronze_records(bronze_path: str, watermark) -> pd.DataFrame:
    conn = duckdb.connect()

    # If no watermark exists (first run), load everything from Bronze
    # If watermark exists, only load records newer than the last processed date
    # This is the incremental loading pattern — only do work that needs doing
    if watermark is None:
        print("[Silver] Loading all Bronze records...")
        df = conn.execute(f"SELECT * FROM '{bronze_path}/*.parquet'").df()
    else:
        print(f"[Silver] Loading Bronze records newer than {watermark}...")
        df = conn.execute(f"""
            SELECT * FROM '{bronze_path}/*.parquet'
            WHERE Date > '{watermark}'
        """).df()

    print(f"[Silver] Loaded {len(df)} new records from Bronze")
    return df

# ── Rename and Drop Columns ───────────────────────────────────────────────────
def rename_and_drop_columns(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    # Build a lookup of ticker -> type from your config
    # This tells us which instruments are mutual funds vs stocks
    ticker_types = {
        ticker: details["type"] 
        for ticker, details in config["holdings"].items()
    }

    # Add the instrument type as a column so we can make per-row decisions
    df["instrument_type"] = df["ticker_symbol"].map(ticker_types)

    # Rename the columns we always keep regardless of instrument type
    df = df.rename(columns={
        "Date": "trade_date",
        "Close": "close_price",
        "High": "high_price",
        "Low": "low_price",
        "Open": "open_price",
        "Volume": "volume"
    })

    # For mutual funds, high/low/open/volume are meaningless so we null them out
    # rather than dropping entirely — this keeps the schema consistent across
    # all instrument types, which makes Gold layer joins much simpler
    mutual_fund_mask = df["instrument_type"] == "mutual_fund"
    df.loc[mutual_fund_mask, ["high_price", "low_price", "open_price", "volume"]] = None

    # Keep all columns now — Gold can decide what to use per instrument type
    columns_to_keep = [
        "trade_date",
        "close_price",
        "high_price",
        "low_price", 
        "open_price",
        "volume",
        "instrument_type",
        "ticker_symbol",
        "ingested_at",
        "source_system",
        "batch_id",
        "ingested_date"
    ]
    df = df[columns_to_keep]
    print(f"[Silver] Columns after rename: {df.columns.tolist()}")
    return df

# ── Cast Types ────────────────────────────────────────────────────────────────
def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    # Cast trade_date from datetime64[ns] (nanosecond timestamp) to plain date
    # You don't need time-of-day precision for daily mutual fund prices
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # Ensure close_price is float64 — it should already be from Bronze
    # but explicit casting here makes Silver's contract crystal clear
    df["close_price"] = df["close_price"].astype(float)

    # Ensure ticker_symbol is a clean string with no extra whitespace
    df["ticker_symbol"] = df["ticker_symbol"].astype(str).str.strip().str.upper()

    print("[Silver] Types cast successfully")
    return df

# ── Deduplicate ───────────────────────────────────────────────────────────────
def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    original_count = len(df)

    # A valid dataset should have exactly one price per ticker per trading day
    # keep="first" means if duplicates exist, keep the first occurrence
    df = df.drop_duplicates(subset=["ticker_symbol", "trade_date"], keep="first")

    dropped = original_count - len(df)
    if dropped > 0:
        print(f"[Silver] Removed {dropped} duplicate rows")
    else:
        print(f"[Silver] No duplicates found")
    return df

# ── Validate ──────────────────────────────────────────────────────────────────
def validate(df: pd.DataFrame) -> pd.DataFrame:
    original_count = len(df)

    # Rule 1: close_price must be greater than zero
    # No real mutual fund is worth nothing or negative
    df = df[df["close_price"] > 0]

    # Rule 2: trade_date cannot be in the future
    # You cannot have tomorrow's price today — this catches API anomalies
    df = df[df["trade_date"] <= date.today()]

    # Rule 3: ticker_symbol cannot be null or empty
    df = df[df["ticker_symbol"].notna() & (df["ticker_symbol"] != "")]

    dropped = original_count - len(df)
    if dropped > 0:
        print(f"[Silver] Filtered out {dropped} invalid rows")
    else:
        print(f"[Silver] All rows passed validation")
    return df

# ── Add Metadata ──────────────────────────────────────────────────────────────
def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    # Add Silver-specific metadata — this is separate from Bronze metadata
    # which is preserved. processed_at tells you when Silver ran on this row.
    # The silver_batch_id distinguishes Silver runs from Bronze runs.
    df["processed_at"] = datetime.now().isoformat()
    df["silver_batch_id"] = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    return df

# ── Save to Parquet ───────────────────────────────────────────────────────────
def save_to_parquet(df: pd.DataFrame, silver_path: str) -> None:
    os.makedirs(silver_path, exist_ok=True)
    output_path = os.path.join(silver_path, f"stock_prices_{date.today().isoformat()}.parquet")

    # Unlike Bronze which is strictly append-only, Silver overwrites today's file
    # if it already exists. This is intentional — if you reprocess today's Bronze
    # data (e.g. to fix a bug), you want Silver to reflect the corrected version
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)
    print(f"[Silver] Written to {output_path} ({len(df)} rows)")

# ── Update Watermark ──────────────────────────────────────────────────────────
def update_watermark(silver_path: str, max_date) -> None:
    # Save the most recent trade_date processed to a simple text file
    # Next time Silver runs, load_watermark() reads this file to know
    # exactly where to pick up from — this is the heart of incremental loading
    watermark_path = os.path.join(silver_path, "_watermark.txt")
    with open(watermark_path, "w") as f:
        f.write(str(max_date))
    print(f"[Silver] Watermark updated to {max_date}")

# ── Orchestrator ──────────────────────────────────────────────────────────────
def run_silver_pipeline() -> None:
    config = load_config()          # load config first
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]

    watermark = load_watermark(silver_path)
    df = load_new_bronze_records(bronze_path, watermark)

    if df.empty:
        print("[Silver] No new records to process — exiting")
        return

    df = rename_and_drop_columns(df, config)   # ← pass config here
    df = cast_types(df)
    df = deduplicate(df)
    df = validate(df)
    df = add_metadata(df)

    save_to_parquet(df, silver_path)

    max_date = df["trade_date"].max()
    update_watermark(silver_path, max_date)

    print("[Silver] Pipeline complete ✅")

if __name__ == "__main__":
    run_silver_pipeline()