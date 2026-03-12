import duckdb

conn = duckdb.connect()

print("=== Row Counts Per Ticker ===")
print(conn.execute("""
    SELECT 
        ticker_symbol,
        instrument_type,
        COUNT(*) as row_count,
        MIN(trade_date) as earliest_date,
        MAX(trade_date) as latest_date,
        ROUND(AVG(close_price), 2) as avg_price
    FROM 'data/silver/*.parquet'
    GROUP BY ticker_symbol, instrument_type
    ORDER BY ticker_symbol
""").df())

print("\n=== Schema Check ===")
print(conn.execute("""
    SELECT * FROM 'data/silver/*.parquet'
    LIMIT 3
""").df())

print("\n=== Null Check on Critical Columns ===")
print(conn.execute("""
    SELECT
        COUNT(*) as total_rows,
        COUNT(*) FILTER (WHERE close_price IS NULL) as null_close,
        COUNT(*) FILTER (WHERE trade_date IS NULL) as null_date,
        COUNT(*) FILTER (WHERE ticker_symbol IS NULL) as null_ticker,
        COUNT(*) FILTER (WHERE close_price <= 0) as invalid_price
    FROM 'data/silver/*.parquet'
""").df())

print("\n=== Duplicate Check ===")
print(conn.execute("""
    SELECT ticker_symbol, trade_date, COUNT(*) as occurrences
    FROM 'data/silver/*.parquet'
    GROUP BY ticker_symbol, trade_date
    HAVING COUNT(*) > 1
    ORDER BY occurrences DESC
    LIMIT 10
""").df())
