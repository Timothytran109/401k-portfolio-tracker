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
    df["year"] = df["full_date"].dt.year
    df["quarter"] = "Q" + df["full_date"].dt.quarter.astype(str)
    df["month"] = df["full_date"].dt.month
    df["month_name"] = df["full_date"].dt.strftime("%B")  # e.g. "March"
    df["day_of_week"] = df["full_date"].dt.strftime("%A") # e.g. "Monday"
    df["is_weekend"] = df["full_date"].dt.dayofweek >= 5  # 5=Saturday, 6=Sunday
    
    # We're not tracking specific market holidays for simplicity
    # In a production system you'd cross-reference a holiday calendar API
    df["is_market_holiday"] = False
    
    # Convert full_date from Timestamp to plain date for consistency
    df["full_date"] = df["full_date"].dt.date
    
    print(f"[Gold] dim_date built — {len(df)} dates from {start_date} to {end_date}")
    return df

def build_dim_stock(config: dict) -> pd.DataFrame:
    holdings = config["holdings"]
    
    rows = []
    # Loop through every holding in your config and build one dim row per fund
    for stock_key, (ticker, details) in enumerate(holdings.items(), start=1):
        rows.append({
            "stock_key": stock_key,          # Surrogate integer key
            "ticker_symbol": ticker,          # Natural business key e.g. "VFIAX"
            "fund_name": details["name"],     # From your config e.g. "Vanguard 500 Index Adm"
            "sector": details.get("sector", "Unknown"),  # From config
            "instrument_type": details["type"],
            "valid_from": date.today(),       # SCD Type 2 — when this record became active
            "valid_to": date(9999, 12, 31),   # Sentinel date meaning "currently active"
            "is_current": True                # Flag for easy filtering of current records
        })
    
    df = pd.DataFrame(rows)
    print(f"[Gold] dim_stock built — {len(df)} funds/stocks")
    return df

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
    
    # CREATE OR REPLACE TABLE means Gold is always a full refresh
    # Unlike Bronze (append-only) and Silver (incremental),
    # Gold is rebuilt completely each run from the authoritative Silver data
    conn.execute("CREATE OR REPLACE TABLE dim_date AS SELECT * FROM dim_date")
    conn.execute("CREATE OR REPLACE TABLE dim_stock AS SELECT * FROM dim_stock")
    conn.execute("CREATE OR REPLACE TABLE fact_stock_prices AS SELECT * FROM fact_stock_prices")
    
    # Verify row counts were written correctly
    for table in ["dim_date", "dim_stock", "fact_stock_prices"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"[Gold] {table}: {count} rows written to {db_path}")
    
    conn.close()
    
def run_gold_pipeline() -> None:
    config = load_config()
    silver_path = config["paths"]["silver"]
    gold_path = config["paths"]["gold"]
    start_date = config["pipeline"]["start_date"]
    end_date = "2035-12-31"  # Generate 10 years of date dimension
    
    # Always build dimensions before facts
    # dim_date has no dependencies — pure calendar generation
    dim_date = build_dim_date(start_date, end_date)
    
    # dim_stock enriches config data with SCD Type 2 tracking
    dim_stock = build_dim_stock(config)
    
    # fact_stock_prices depends on both dimensions existing first
    fact_stock_prices = build_fact_stock_prices(
        silver_path, dim_stock, dim_date, config
    )
    
    # Write everything to the DuckDB database file
    save_to_duckdb(dim_date, dim_stock, fact_stock_prices, gold_path)
    
    print("[Gold] Pipeline complete ✅")

if __name__ == "__main__":
    run_gold_pipeline()