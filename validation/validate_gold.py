import duckdb

# Connect to your actual Gold database file
conn = duckdb.connect("data/gold/portfolio.duckdb")

print("=== dim_date sample ===")
print(conn.execute("""
    SELECT date_key, full_date, year, quarter, month_name, day_of_week, is_weekend
    FROM dim_date
    WHERE full_date BETWEEN '2025-03-10' AND '2025-03-14'
    ORDER BY full_date
""").df())

print("\n=== dim_stock ===")
print(conn.execute("""
    SELECT stock_key, ticker_symbol, fund_name, sector, instrument_type, is_current
    FROM dim_stock
    ORDER BY stock_key
""").df())

print("\n=== fact_stock_prices sample ===")
print(conn.execute("""
    SELECT 
        f.price_key,
        d.full_date,
        s.ticker_symbol,
        f.close_price,
        f.daily_return_pct,
        f.portfolio_value
    FROM fact_stock_prices f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_stock s ON f.stock_key = s.stock_key
    WHERE d.full_date >= '2026-03-09'
    ORDER BY d.full_date, s.ticker_symbol
    LIMIT 20
""").df())

print("\n=== Portfolio Value By Date (Business Query Test) ===")
print(conn.execute("""
    SELECT
        d.full_date,
        d.quarter,
        ROUND(SUM(f.portfolio_value), 2) as total_portfolio_value,
        ROUND(AVG(f.daily_return_pct), 4) as avg_daily_return_pct
    FROM fact_stock_prices f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.full_date, d.quarter
    ORDER BY d.full_date DESC
    LIMIT 10
""").df())

print("\n=== Best Performing Funds (All Time) ===")
print(conn.execute("""
    SELECT
        s.ticker_symbol,
        s.fund_name,
        s.sector,
        ROUND(AVG(f.daily_return_pct), 4) as avg_daily_return,
        ROUND(MAX(f.close_price), 2) as max_price,
        ROUND(MIN(f.close_price), 2) as min_price
    FROM fact_stock_prices f
    JOIN dim_stock s ON f.stock_key = s.stock_key
    GROUP BY s.ticker_symbol, s.fund_name, s.sector
    ORDER BY avg_daily_return DESC
""").df())

conn.close()