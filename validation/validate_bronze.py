import duckdb

# DuckDB can query Parquet files directly — no loading needed
conn = duckdb.connect()

# Check row counts per ticker
print("=== Row Counts Per Ticker ===")
print(conn.execute("""
    SELECT 
        ticker_symbol,
        COUNT(*) as row_count,
        MIN(Date) as earliest_date,
        MAX(Date) as latest_date
    FROM 'data/bronze/*.parquet'
    GROUP BY ticker_symbol
    ORDER BY ticker_symbol
""").df())

# Check metadata columns exist
print("\n=== Sample Row With Metadata ===")
print(conn.execute("""
    SELECT ticker_symbol, Date, Close, ingested_at, source_system, batch_id
    FROM 'data/bronze/*.parquet'
    LIMIT 5
""").df())

# Check for nulls in critical columns
print("\n=== Null Check ===")
print(conn.execute("""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(*) FILTER (WHERE Close IS NULL) as null_close,
        COUNT(*) FILTER (WHERE ticker_symbol IS NULL) as null_ticker,
        COUNT(*) FILTER (WHERE Date IS NULL) as null_date
    FROM 'data/bronze/*.parquet'
""").df())