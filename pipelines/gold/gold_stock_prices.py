import duckdb
import pandas as pd
import yaml
import os
from datetime import datetime, date

def load_config() -> dict:
    with open("pipeline_config.yaml", "r") as f:
        return yaml.safe_load(f)

def build_dim_date(start_date: str, end_date: str) -> pd.DataFrame:
    # Generate every single calendar date between start and end
    # This creates a DatetimeIndex — think of it as a list of dates
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    df = pd.DataFrame({"full_date": date_range})

    # date_key is the date formatted as an integer YYYYMMDD
    # e.g. 2025-03-10 becomes 20250310
    # We use an integer rather than a string because integers
    # are faster to join on in DuckDB — this is a Kimball best practice
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)

    # Extract all the calendar attributes that make dim_date valuable
    # These are pre-calculated so your dashboard queries never have to
    # derive them on the fly — they're just sitting there ready to use
    df["year"]        = df["full_date"].dt.year
    df["quarter"]     = "Q" + df["full_date"].dt.quarter.astype(str)
    df["month"]       = df["full_date"].dt.month
    df["month_name"]  = df["full_date"].dt.strftime("%B")   # e.g. "March"
    df["day_of_week"] = df["full_date"].dt.strftime("%A")   # e.g. "Monday"
    df["is_weekend"]  = df["full_date"].dt.dayofweek >= 5   # 5=Saturday, 6=Sunday

    # We're not tracking specific market holidays for simplicity
    # In a production system you'd cross-reference a holiday calendar API
    df["is_market_holiday"] = False

    # Convert full_date from Timestamp to plain date for consistency
    df["full_date"] = df["full_date"].dt.date

    print(f"[Gold] dim_date built — {len(df)} dates from {start_date} to {end_date}")
    return df


def build_dim_stock_scd2(config: dict, db_path: str) -> pd.DataFrame:
    """
    Build dim_stock using SCD Type 2 logic.

    SCD Type 2 (Slowly Changing Dimension Type 2) preserves the full history
    of a dimension record when its attributes change. Rather than overwriting
    the old record, we:
        1. Close the old record  → set valid_to = today, is_current = False
        2. Insert a new record   → valid_from = today, valid_to = 9999-12-31, is_current = True

    This means fact rows always point to the version of a fund that was
    accurate at the time of the trade — historical accuracy is preserved
    even if a fund changes its name, sector, or classification later.

    The three SCD Type 2 tracking columns are:
        valid_from  — date this version of the record became active
        valid_to    — date this version expired (9999-12-31 = still active)
        is_current  — quick boolean filter for the latest version
    """
    holdings     = config["holdings"]
    today        = date.today()
    sentinel     = date(9999, 12, 31)   # sentinel date meaning "still active"

    # ── Step 1: Build the incoming records from config ─────────────────────────
    # These represent what dim_stock SHOULD look like today based on config.
    # The SCD attributes (valid_from, valid_to, is_current) are placeholders
    # for now — we'll set them correctly after comparing with existing data.
    incoming = {}
    for ticker, details in holdings.items():
        incoming[ticker] = {
            "ticker_symbol":  ticker,
            "fund_name":      details["name"],
            "sector":         details.get("sector", "Unknown"),
            "instrument_type": details["type"],
        }

    # ── Step 2: Load existing dim_stock from DuckDB if it exists ──────────────
    # If DuckDB doesn't exist yet (first run), we skip straight to inserting
    # all records as brand new with valid_from = pipeline start date.
    db_exists     = os.path.exists(db_path)
    existing_rows = []

    if db_exists:
        try:
            conn = duckdb.connect(db_path, read_only=True)
            existing_rows = conn.execute("SELECT * FROM dim_stock").df().to_dict("records")
            conn.close()
            print(f"[Gold] SCD2 — loaded {len(existing_rows)} existing dim_stock rows")
        except Exception:
            # Table doesn't exist yet even if DB file exists
            print("[Gold] SCD2 — dim_stock table not found, treating as first run")
            existing_rows = []

    # ── Step 3: Build a lookup of currently active rows ───────────────────────
    # current_active maps ticker_symbol → the row that has is_current = True
    # This is what we'll compare against incoming config values.
    current_active = {
        row["ticker_symbol"]: row
        for row in existing_rows
        if row.get("is_current", False)
    }

    # ── Step 4: Determine the highest existing stock_key ──────────────────────
    # New rows need surrogate keys that don't collide with existing ones.
    # If this is the first run, we start at 1.
    max_key = max(
        (row["stock_key"] for row in existing_rows),
        default=0
    )

    # ── Step 5: SCD Type 2 comparison logic ───────────────────────────────────
    # We compare each incoming record against the currently active row for
    # that ticker. Three outcomes are possible:
    #
    #   A) UNCHANGED  → attributes are identical → keep existing row, no action
    #   B) CHANGED    → attributes differ → close old row, insert new row
    #   C) NEW        → ticker doesn't exist yet → insert new row
    #
    # The fields we track for changes are fund_name, sector, instrument_type.
    # If any of these change, it triggers a Type 2 update.
    TRACKED_FIELDS = ["fund_name", "sector", "instrument_type"]

    output_rows = []   # final list of all rows to write to dim_stock

    # Keep all historical (non-current) rows unchanged — they are the audit trail
    for row in existing_rows:
        if not row.get("is_current", False):
            output_rows.append(row)

    for ticker, new_attrs in incoming.items():

        if ticker in current_active:
            existing = current_active[ticker]

            # Check if any tracked attribute has changed
            changed = any(
                str(existing.get(field, "")) != str(new_attrs.get(field, ""))
                for field in TRACKED_FIELDS
            )

            if not changed:
                # ── Case A: UNCHANGED ──────────────────────────────────────
                # Nothing changed — keep the existing row exactly as-is.
                # valid_from stays the same (when this version first became active).
                output_rows.append(existing)
                print(f"[Gold] SCD2 — {ticker}: unchanged, keeping existing row")

            else:
                # ── Case B: CHANGED ────────────────────────────────────────
                # Something changed. Close the old row by setting:
                #   valid_to   = yesterday (last day this version was accurate)
                #   is_current = False
                # Then insert a new row with:
                #   valid_from = today (when the new version became active)
                #   valid_to   = 9999-12-31 (sentinel — still active)
                #   is_current = True
                #   stock_key  = new surrogate key (so fact rows can distinguish versions)
                closed_row = dict(existing)
                closed_row["valid_to"]   = today
                closed_row["is_current"] = False
                output_rows.append(closed_row)

                max_key += 1
                new_row = {
                    "stock_key":      max_key,
                    "ticker_symbol":  ticker,
                    "fund_name":      new_attrs["fund_name"],
                    "sector":         new_attrs["sector"],
                    "instrument_type": new_attrs["instrument_type"],
                    "valid_from":     today,
                    "valid_to":       sentinel,
                    "is_current":     True,
                }
                output_rows.append(new_row)
                print(f"[Gold] SCD2 — {ticker}: CHANGED — closed old row, inserted new (stock_key={max_key})")

        else:
            # ── Case C: NEW TICKER ─────────────────────────────────────────
            # This ticker doesn't exist in dim_stock at all yet.
            # Insert a brand new row. valid_from is the pipeline start date
            # on first run, or today if added mid-project.
            max_key += 1
            new_row = {
                "stock_key":      max_key,
                "ticker_symbol":  ticker,
                "fund_name":      new_attrs["fund_name"],
                "sector":         new_attrs["sector"],
                "instrument_type": new_attrs["instrument_type"],
                "valid_from":     today,
                "valid_to":       sentinel,
                "is_current":     True,
            }
            output_rows.append(new_row)
            print(f"[Gold] SCD2 — {ticker}: NEW — inserted (stock_key={max_key})")

    dim_stock = pd.DataFrame(output_rows)

    # Ensure correct column types for DuckDB
    dim_stock["stock_key"]  = dim_stock["stock_key"].astype(int)
    dim_stock["valid_from"] = pd.to_datetime(dim_stock["valid_from"]).dt.date
    dim_stock["valid_to"]   = pd.to_datetime(dim_stock["valid_to"]).dt.date
    dim_stock["is_current"] = dim_stock["is_current"].astype(bool)

    current_count = dim_stock["is_current"].sum()
    total_count   = len(dim_stock)
    print(f"[Gold] dim_stock built — {current_count} current rows, {total_count} total (includes history)")
    return dim_stock


def build_fact_stock_prices(
    silver_path: str,
    dim_stock: pd.DataFrame,
    dim_date: pd.DataFrame,
    config: dict
) -> pd.DataFrame:
    conn = duckdb.connect()

    # Register your dimension DataFrames as virtual tables that DuckDB can query
    # This is one of DuckDB's superpowers — you can mix SQL with pandas DataFrames
    conn.register("dim_stock_df", dim_stock)
    conn.register("dim_date_df", dim_date)

    # Build a baseline value lookup from your config
    # This tells Gold how much each holding was worth at the start
    # so it can calculate the current portfolio value from price changes
    baseline = {
        ticker: details["value_usd"]
        for ticker, details in config["holdings"].items()
    }
    baseline_df = pd.DataFrame([
        {"ticker_symbol": k, "baseline_value_usd": v}
        for k, v in baseline.items()
    ])
    conn.register("baseline_df", baseline_df)

    fact_df = conn.execute(f"""
        WITH silver AS (
            -- Load all Silver records
            SELECT * FROM read_parquet('{silver_path}/*.parquet')
        ),

        with_keys AS (
            -- Replace ticker_symbol string with surrogate stock_key integer
            -- Replace trade_date with date_key integer
            -- This is the surrogate key lookup — the core Gold operation
            -- We join to is_current = true so we always use the latest
            -- version of each fund's metadata for new fact rows
            SELECT
                d.date_key,
                s.stock_key,
                sv.close_price,
                sv.ingested_at,
                sv.processed_at,
                sv.ticker_symbol,
                sv.trade_date
            FROM silver sv
            JOIN dim_stock_df s
                ON sv.ticker_symbol = s.ticker_symbol
                AND s.is_current = true
            JOIN dim_date_df d
                ON CAST(sv.trade_date AS DATE) = d.full_date
        ),

        with_returns AS (
            -- Calculate daily_return_pct using LAG() window function
            -- LAG(close_price, 1) means "give me the close_price from the
            -- previous row for this same ticker, ordered by date"
            -- This is how you look backwards in time across rows in SQL
            SELECT
                *,
                ROUND(
                    (close_price - LAG(close_price, 1) OVER (
                        PARTITION BY stock_key    -- restart for each ticker
                        ORDER BY trade_date        -- look at previous trading day
                    )) / NULLIF(LAG(close_price, 1) OVER (
                        PARTITION BY stock_key
                        ORDER BY trade_date
                    ), 0) * 100,
                    4
                ) as daily_return_pct
            FROM with_keys
        )

        -- Final SELECT: join to baseline to calculate portfolio_value
        -- portfolio_value = baseline * (1 + cumulative return)
        -- For simplicity we calculate it as:
        -- baseline_value * (current_price / first_ever_price_for_this_ticker)
        SELECT
            ROW_NUMBER() OVER (ORDER BY r.trade_date, r.stock_key) as price_key,
            r.date_key,
            r.stock_key,
            r.close_price,
            r.daily_return_pct,
            ROUND(
                b.baseline_value_usd * (
                    r.close_price / FIRST_VALUE(r.close_price) OVER (
                        PARTITION BY r.stock_key
                        ORDER BY r.trade_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    )
                ), 2
            ) as portfolio_value,
            r.ingested_at,
            r.processed_at
        FROM with_returns r
        JOIN baseline_df b ON r.ticker_symbol = b.ticker_symbol
        ORDER BY r.trade_date, r.stock_key
    """).df()

    conn.close()
    print(f"[Gold] fact_stock_prices built — {len(fact_df)} rows")
    return fact_df


def save_to_duckdb(
    dim_date: pd.DataFrame,
    dim_stock: pd.DataFrame,
    fact_stock_prices: pd.DataFrame,
    gold_path: str
) -> None:
    os.makedirs(gold_path, exist_ok=True)
    db_path = os.path.join(gold_path, "portfolio.duckdb")

    conn = duckdb.connect(db_path)

    # dim_date and fact_stock_prices are always full refresh —
    # dim_date is a pure calendar, fact is rebuilt from Silver each run.
    conn.execute("CREATE OR REPLACE TABLE dim_date AS SELECT * FROM dim_date")
    conn.execute("CREATE OR REPLACE TABLE fact_stock_prices AS SELECT * FROM fact_stock_prices")

    # dim_stock uses SCD Type 2 — we write the full history (current + closed rows)
    # that was assembled by build_dim_stock_scd2(). We use CREATE OR REPLACE here
    # because build_dim_stock_scd2() already merged old and new rows in Python —
    # the DataFrame passed in IS the complete authoritative dim_stock state.
    conn.execute("CREATE OR REPLACE TABLE dim_stock AS SELECT * FROM dim_stock")

    # Verify row counts were written correctly
    for table in ["dim_date", "dim_stock", "fact_stock_prices"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"[Gold] {table}: {count} rows written to {db_path}")

    conn.close()


def run_gold_pipeline() -> None:
    config     = load_config()
    silver_path = config["paths"]["silver"]
    gold_path   = config["paths"]["gold"]
    start_date  = config["pipeline"]["start_date"]
    end_date    = "2035-12-31"   # Generate 10 years of date dimension
    db_path     = os.path.join(gold_path, "portfolio.duckdb")

    # Always build dimensions before facts
    # dim_date has no dependencies — pure calendar generation
    dim_date = build_dim_date(start_date, end_date)

    # dim_stock uses SCD Type 2 — reads existing DB to preserve history
    dim_stock = build_dim_stock_scd2(config, db_path)

    # fact_stock_prices depends on both dimensions existing first
    fact_stock_prices = build_fact_stock_prices(
        silver_path, dim_stock, dim_date, config
    )

    # Write everything to the DuckDB database file
    save_to_duckdb(dim_date, dim_stock, fact_stock_prices, gold_path)

    print("[Gold] Pipeline complete ✅")


if __name__ == "__main__":
    run_gold_pipeline()