import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
import os
from datetime import datetime, date

# ── Config ────────────────────────────────────────────────────────────────────
def load_config():
    with open("pipeline_config.yaml", "r") as f:
        return yaml.safe_load(f)

# ── Extract ───────────────────────────────────────────────────────────────────
def extract_stock_prices(tickers: list, start_date: str) -> pd.DataFrame:
    print(f"[Bronze] Extracting data for {len(tickers)} tickers...")
    
    all_data = []
    for ticker in tickers:
        try:
            raw = yf.download(ticker, start=start_date, auto_adjust=True)
            if raw.empty:
                print(f"  ⚠️  No data returned for {ticker} — skipping")
                continue

            raw = raw.reset_index()
            raw.columns = [col[0] if isinstance(col, tuple) else col for col in raw.columns]
            raw["ticker_symbol"] = ticker
            all_data.append(raw)
            print(f"  ✅ {ticker}: {len(raw)} rows")

        except Exception as e:
            print(f"  ❌ {ticker} failed: {e}")

    if not all_data:
        raise ValueError("No data extracted for any ticker")

    df = pd.concat(all_data, ignore_index=True)
    return df

# ── Add Metadata ──────────────────────────────────────────────────────────────
def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    batch_id = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    df["ingested_at"] = datetime.now().isoformat()
    df["source_system"] = "yfinance"
    df["batch_id"] = batch_id
    df["ingested_date"] = date.today().isoformat()
    return df

# ── Load ──────────────────────────────────────────────────────────────────────
def load_to_parquet(df: pd.DataFrame, bronze_path: str):
    ingested_date = date.today().isoformat()
    output_path = os.path.join(bronze_path, f"stock_prices_{ingested_date}.parquet")

    # Append-only — never overwrite existing files
    if os.path.exists(output_path):
        print(f"[Bronze] File already exists for today ({ingested_date}) — skipping write")
        return

    os.makedirs(bronze_path, exist_ok=True)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)
    print(f"[Bronze] Written to {output_path} ({len(df)} rows)")

# ── Main ──────────────────────────────────────────────────────────────────────
def run_bronze_pipeline():
    config = load_config()
    tickers = config["tickers"]
    start_date = config["pipeline"]["start_date"]
    bronze_path = config["paths"]["bronze"]

    df = extract_stock_prices(tickers, start_date)
    df = add_metadata(df)
    load_to_parquet(df, bronze_path)
    print("[Bronze] Pipeline complete ✅")

if __name__ == "__main__":
    run_bronze_pipeline()