"""
401k Portfolio Tracker — Streamlit Dashboard
=============================================
Reads from data/gold/portfolio.duckdb (Gold layer star schema)

Run with:
    streamlit run dashboard/app.py

Requirements:
    pip install streamlit duckdb pandas plotly
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import date

# ══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# Must be the very first Streamlit call — sets the browser tab title,
# icon, and tells Streamlit to use the full screen width.
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="401k Portfolio Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# CUSTOM CSS
# Streamlit renders inside an HTML page, so we can inject CSS to override
# the default styles. This gives the dashboard a dark, monospaced financial
# terminal aesthetic — think Bloomberg, not a generic BI tool.
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500&family=Cormorant+Garamond:ital,wght@0,300;0,600;1,300&display=swap');

/* Base font for all UI elements */
html, body, [class*="css"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
}

/* Headings use a refined serif for contrast against the mono body */
h1, h2, h3 {
    font-family: 'Cormorant Garamond', serif !important;
    font-weight: 300 !important;
    letter-spacing: 0.02em;
}

/* ── KPI cards ── */
.kpi-wrap {
    background: #0f1117;
    border: 1px solid #1e2130;
    border-top: 2px solid #3b82f6;
    border-radius: 4px;
    padding: 1.1rem 1.3rem 0.9rem;
    height: 100%;
}
.kpi-label {
    font-size: 0.58rem;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: #4b5563;
    margin-bottom: 6px;
}
.kpi-val {
    font-size: 1.65rem;
    font-family: 'Cormorant Garamond', serif;
    font-weight: 600;
    color: #f1f5f9;
    line-height: 1;
}
.kpi-sub { font-size: 0.65rem; margin-top: 6px; color: #4b5563; }
.up      { color: #10b981; }
.down    { color: #ef4444; }
.flat    { color: #6b7280; }

/* ── Section dividers ── */
.sec {
    font-size: 0.55rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: #374151;
    border-bottom: 1px solid #1e2130;
    padding-bottom: 4px;
    margin: 2rem 0 0.9rem;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #080a0f;
    border-right: 1px solid #1e2130;
}
section[data-testid="stSidebar"] label {
    font-size: 0.6rem !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    color: #4b5563 !important;
}

/* Hide Streamlit branding */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# THEME CONSTANTS
# Centralising colours here means we only change them in one place if we
# ever want a different look. These get passed into Plotly chart configs.
# ══════════════════════════════════════════════════════════════════════════════
C = {
    "bg":      "rgba(0,0,0,0)",   # transparent — lets CSS background show through
    "surface": "#0f1117",
    "border":  "#1e2130",
    "text":    "#f1f5f9",
    "muted":   "#4b5563",
    "green":   "#10b981",
    "red":     "#ef4444",
    "blue":    "#3b82f6",
    "yellow":  "#f59e0b",
}

# Base Plotly layout that every chart inherits — ensures visual consistency
BASE_LAYOUT = dict(
    paper_bgcolor=C["bg"],
    plot_bgcolor=C["bg"],
    font=dict(family="IBM Plex Mono, monospace", color=C["muted"], size=10),
    xaxis=dict(gridcolor=C["border"], zerolinecolor=C["border"], showgrid=True),
    yaxis=dict(gridcolor=C["border"], zerolinecolor=C["border"], showgrid=True),
    margin=dict(l=8, r=8, t=32, b=8),
    hoverlabel=dict(
        bgcolor=C["surface"],
        bordercolor=C["border"],
        font_color=C["text"],
        font_family="IBM Plex Mono",
    ),
    legend=dict(
        bgcolor=C["bg"],
        bordercolor=C["border"],
        borderwidth=1,
        font=dict(size=10),
    ),
)

# 11 distinct colours — one per ticker — colourblind-considerate palette
TICKER_COLORS = [
    "#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6",
    "#06b6d4", "#f97316", "#84cc16", "#ec4899", "#14b8a6", "#94a3b8",
]

# ══════════════════════════════════════════════════════════════════════════════
# DATABASE CONNECTION
# @st.cache_resource means Streamlit creates this connection once and reuses it
# across every user interaction. Without caching, every button click or filter
# change would open a brand new DB connection — wasteful and slow.
# ══════════════════════════════════════════════════════════════════════════════
DB_PATH = Path(__file__).parent.parent / "data" / "gold" / "portfolio.duckdb"

def get_conn():
    """Open a fresh connection to the Gold DuckDB file each time."""
    if not DB_PATH.exists():
        return None
    return duckdb.connect(str(DB_PATH), read_only=True)

conn = get_conn()

# Guard: if the DB doesn't exist yet, show a helpful error and stop rendering
if conn is None:
    st.error(f"**DuckDB not found** at `{DB_PATH}`")
    st.info("Run the Gold pipeline first to generate the database:")
    st.code("python pipelines/gold/gold_stock_prices.py", language="bash")
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADERS
# Each function runs one SQL query against the Gold star schema and returns
# a pandas DataFrame. @st.cache_data(ttl=300) means the result is cached for
# 5 minutes — so fast filter interactions don't re-query the DB every time.
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_fact() -> pd.DataFrame:
    """
    Main dataset: joins fact_stock_prices → dim_date → dim_stock.
    This is the star schema join in action — the fact table sits in the middle,
    and we pull descriptive attributes from both dimension tables in one query.
    Only selects current dim_stock records (is_current = TRUE) to avoid
    accidentally pulling historical SCD Type 2 rows.
    """
    return conn.execute("""
        SELECT
            f.close_price,
            f.daily_return_pct,
            f.portfolio_value,
            f.processed_at,
            -- From dim_date: human-readable calendar attributes
            d.full_date        AS trade_date,
            d.year,
            d.quarter,
            d.month_name,
            d.is_weekend,
            -- From dim_stock: fund metadata
            s.ticker_symbol,
            s.fund_name,
            s.sector,
            s.instrument_type
        FROM fact_stock_prices f
        JOIN dim_date  d ON f.date_key  = d.date_key
        JOIN dim_stock s ON f.stock_key = s.stock_key
        WHERE s.is_current = TRUE
        ORDER BY d.full_date, s.ticker_symbol
    """).df()

@st.cache_data(ttl=300)
def load_freshness() -> dict:
    """
    Reads pipeline metadata from the fact table to show how stale the data is.
    Shows you thought about operational concerns, not just data modeling.
    """
    try:
        row = conn.execute("""
            SELECT
                MAX(d.full_date)   AS latest_trade,
                MAX(f.processed_at) AS last_processed
            FROM fact_stock_prices f
            JOIN dim_date d ON f.date_key = d.date_key
        """).fetchone()
        return {"latest_trade": row[0], "last_processed": row[1]}
    except Exception:
        return {}

# Load everything upfront — filters are applied in Python, not via re-queries
fact      = load_fact()
freshness = load_freshness()

# Coerce trade_date to proper datetime so pandas date comparisons work
fact["trade_date"] = pd.to_datetime(fact["trade_date"])

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — FILTERS
# The sidebar holds all the controls that let users slice the data.
# Every widget here produces a Python value that we use below to filter
# the DataFrame — no additional DB queries needed.
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 📈 401k Tracker")
    st.caption(f"`{DB_PATH.name}`")
    st.divider()

    min_dt = fact["trade_date"].min().date()
    max_dt = fact["trade_date"].max().date()
    date_range = st.date_input(
        "Date Range",
        value=(min_dt, max_dt),
        min_value=min_dt,
        max_value=max_dt,
    )

    all_types = sorted(fact["instrument_type"].dropna().unique())
    sel_types = st.multiselect("Instrument Type", all_types, default=all_types)

    all_sectors = sorted(fact["sector"].dropna().unique())
    sel_sectors = st.multiselect("Sector", all_sectors, default=all_sectors)

    all_tickers = sorted(fact["ticker_symbol"].unique())
    sel_tickers = st.multiselect("Tickers", all_tickers, default=all_tickers)

    st.divider()

    # Pipeline freshness panel
    st.markdown(
        "<div style='font-size:0.58rem;letter-spacing:0.18em;text-transform:uppercase;"
        "color:#4b5563;margin-bottom:8px'>Pipeline Status</div>",
        unsafe_allow_html=True,
    )
    if freshness:
        lt = freshness.get("latest_trade")
        lp = freshness.get("last_processed")
        if lt:
            days_stale = (pd.Timestamp.today().normalize() - pd.Timestamp(str(lt))).days
            color = "#10b981" if days_stale <= 1 else ("#f59e0b" if days_stale <= 3 else "#ef4444")
            st.markdown(
                f"<span style='color:{color}'>● Latest trade: <b>{lt}</b></span><br>"
                f"<span style='color:#4b5563;font-size:0.65rem'>{days_stale} day(s) ago</span>",
                unsafe_allow_html=True,
            )
        if lp:
            st.caption(f"Last processed: {str(lp)[:19]}")
    else:
        st.caption("No freshness data found.")

# ══════════════════════════════════════════════════════════════════════════════
# APPLY FILTERS
# All filtering happens here in pandas — we built one big DataFrame upfront
# and now slice it based on whatever the sidebar widgets currently say.
# ══════════════════════════════════════════════════════════════════════════════
try:
    start_dt = pd.Timestamp(date_range[0])
    end_dt   = pd.Timestamp(date_range[1])
except Exception:
    start_dt = fact["trade_date"].min()
    end_dt   = fact["trade_date"].max()

df = fact[
    (fact["trade_date"]          >= start_dt) &
    (fact["trade_date"]          <= end_dt)   &
    (fact["instrument_type"].isin(sel_types))  &
    (fact["sector"].isin(sel_sectors))         &
    (fact["ticker_symbol"].isin(sel_tickers))
].copy()

if df.empty:
    st.warning("No data matches your current filters. Try adjusting the sidebar.")
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
# KPI CALCULATIONS
# These headline numbers react to sidebar filter changes automatically because
# they're derived from the filtered DataFrame, not from raw DB queries.
# ══════════════════════════════════════════════════════════════════════════════
portfolio_by_date = (
    df.groupby("trade_date")["portfolio_value"]
    .sum()
    .reset_index()
    .sort_values("trade_date")
)

latest_total   = portfolio_by_date["portfolio_value"].iloc[-1]
earliest_total = portfolio_by_date["portfolio_value"].iloc[0]
period_return  = ((latest_total - earliest_total) / earliest_total * 100) if earliest_total else 0

latest_date = df["trade_date"].max()
prev_dates  = df[df["trade_date"] < latest_date]["trade_date"]
if not prev_dates.empty:
    prev_date  = prev_dates.max()
    prev_total = portfolio_by_date[portfolio_by_date["trade_date"] == prev_date]["portfolio_value"].values[0]
    day_change   = latest_total - prev_total
    day_change_p = (day_change / prev_total * 100) if prev_total else 0
else:
    day_change, day_change_p = 0.0, 0.0

avg_daily_ret = df["daily_return_pct"].dropna().mean()
n_tickers     = df["ticker_symbol"].nunique()

# ══════════════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("# 401k Portfolio Tracker")
st.markdown(
    f"<span style='font-size:0.65rem;color:#4b5563;letter-spacing:0.1em'>"
    f"{start_dt.date()} → {end_dt.date()}&nbsp;&nbsp;·&nbsp;&nbsp;"
    f"{n_tickers} instruments&nbsp;&nbsp;·&nbsp;&nbsp;"
    f"{len(df):,} data points</span>",
    unsafe_allow_html=True,
)
st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# KPI ROW — 4 headline metrics
# We use st.markdown with custom HTML to get full control over typography.
# Streamlit's native st.metric widget exists but can't be styled this precisely.
# ══════════════════════════════════════════════════════════════════════════════
def kpi(label, value, sub, sub_class="flat"):
    """Renders a single KPI card using our custom CSS classes."""
    return (
        f"<div class='kpi-wrap'>"
        f"<div class='kpi-label'>{label}</div>"
        f"<div class='kpi-val'>{value}</div>"
        f"<div class='kpi-sub {sub_class}'>{sub}</div>"
        f"</div>"
    )

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(kpi(
        "Total Portfolio Value",
        f"${latest_total:,.2f}",
        f"{'▲' if period_return >= 0 else '▼'} {period_return:+.2f}% over period",
        "up" if period_return >= 0 else "down",
    ), unsafe_allow_html=True)

with k2:
    st.markdown(kpi(
        "Day Change",
        f"${day_change:+,.2f}",
        f"{'▲' if day_change_p >= 0 else '▼'} {day_change_p:+.2f}% vs prior day",
        "up" if day_change >= 0 else "down",
    ), unsafe_allow_html=True)

with k3:
    st.markdown(kpi(
        "Avg Daily Return",
        f"{avg_daily_ret:+.3f}%",
        "mean across all selected tickers",
        "up" if avg_daily_ret >= 0 else "down",
    ), unsafe_allow_html=True)

with k4:
    n_funds  = df[df["instrument_type"] == "mutual_fund"]["ticker_symbol"].nunique()
    n_stocks = df[df["instrument_type"] == "stock"]["ticker_symbol"].nunique()
    st.markdown(kpi(
        "Instruments",
        str(n_tickers),
        f"{n_funds} funds · {n_stocks} stocks",
        "flat",
    ), unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# CHART 1 — TOTAL PORTFOLIO VALUE OVER TIME
# Aggregate value of the whole portfolio as a filled line chart over time.
# This answers the most fundamental question: is my 401k growing?
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("<div class='sec'>Portfolio Value Over Time</div>", unsafe_allow_html=True)

fig_total = go.Figure()
fig_total.add_trace(go.Scatter(
    x=portfolio_by_date["trade_date"],
    y=portfolio_by_date["portfolio_value"],
    mode="lines",
    name="Total",
    line=dict(color=C["blue"], width=2.5),
    fill="tozeroy",
    fillcolor="rgba(59,130,246,0.06)",
    hovertemplate="<b>%{x|%b %d, %Y}</b><br>$%{y:,.2f}<extra></extra>",
))
fig_total.update_layout(**BASE_LAYOUT, height=260)
fig_total.update_yaxes(tickprefix="$", tickformat=",.0f")
st.plotly_chart(fig_total, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# CHART 2 (left) — PER-TICKER VALUE OVER TIME
# CHART 3 (right) — ALLOCATION BY SECTOR (donut)
# Side by side — trajectory on the left, composition on the right.
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("<div class='sec'>Breakdown</div>", unsafe_allow_html=True)
col_l, col_r = st.columns([3, 2])

with col_l:
    st.caption("Per-Ticker Portfolio Value")

    ticker_by_date = (
        df.groupby(["trade_date", "ticker_symbol"])["portfolio_value"]
        .sum()
        .reset_index()
    )
    ticker_list = sorted(ticker_by_date["ticker_symbol"].unique())
    color_map   = {t: TICKER_COLORS[i % len(TICKER_COLORS)] for i, t in enumerate(ticker_list)}

    fig_tickers = px.line(
        ticker_by_date,
        x="trade_date",
        y="portfolio_value",
        color="ticker_symbol",
        color_discrete_map=color_map,
        labels={"trade_date": "", "portfolio_value": "Value ($)", "ticker_symbol": "Ticker"},
    )
    fig_tickers.update_traces(line_width=1.5)
    fig_tickers.update_layout(**BASE_LAYOUT, height=300)
    fig_tickers.update_yaxes(tickprefix="$", tickformat=",.0f")
    st.plotly_chart(fig_tickers, use_container_width=True)

with col_r:
    st.caption("Allocation by Sector (latest date)")

    # Use only the most recent date so the pie reflects current allocation
    latest_df    = df[df["trade_date"] == latest_date]
    sector_alloc = (
        latest_df.groupby("sector")["portfolio_value"]
        .sum()
        .reset_index()
        .rename(columns={"portfolio_value": "value"})
    )

    fig_pie = px.pie(
        sector_alloc,
        names="sector",
        values="value",
        color_discrete_sequence=TICKER_COLORS,
        hole=0.45,
    )
    fig_pie.update_traces(
        textfont=dict(family="IBM Plex Mono", size=9),
        hovertemplate="<b>%{label}</b><br>$%{value:,.2f} (%{percent})<extra></extra>",
    )
    fig_pie.update_layout(**BASE_LAYOUT, height=300, showlegend=True)
    st.plotly_chart(fig_pie, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# CHART 4 — PERFORMANCE: TOTAL RETURN + AVG DAILY RETURN
# For each ticker, compute total return = (last close - first close) / first close
# Green bars = positive return, red bars = negative. Instantly readable.
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("<div class='sec'>Performance</div>", unsafe_allow_html=True)
col_perf, col_ret = st.columns([3, 2])

with col_perf:
    st.caption("Total Return by Ticker (selected period)")

    perf = (
        df.sort_values("trade_date")
        .groupby("ticker_symbol")
        .agg(
            first_close=("close_price", "first"),
            last_close=("close_price",  "last"),
        )
        .reset_index()
    )
    perf["total_return_pct"] = (
        (perf["last_close"] - perf["first_close"]) / perf["first_close"] * 100
    ).round(2)
    perf = perf.sort_values("total_return_pct", ascending=True)
    perf["color"] = perf["total_return_pct"].apply(
        lambda x: C["green"] if x >= 0 else C["red"]
    )

    fig_perf = go.Figure(go.Bar(
        x=perf["total_return_pct"],
        y=perf["ticker_symbol"],
        orientation="h",
        marker_color=perf["color"],
        text=perf["total_return_pct"].apply(lambda x: f"{x:+.2f}%"),
        textposition="outside",
        textfont=dict(size=9),
        hovertemplate="<b>%{y}</b><br>%{x:+.2f}%<extra></extra>",
    ))
    fig_perf.update_layout(**BASE_LAYOUT, height=360)
    fig_perf.update_xaxes(
        ticksuffix="%",
        zeroline=True,
        zerolinecolor=C["muted"],
        zerolinewidth=1,
    )
    st.plotly_chart(fig_perf, use_container_width=True)

with col_ret:
    st.caption("Avg Daily Return by Ticker")

    avg_ret = (
        df.groupby("ticker_symbol")["daily_return_pct"]
        .mean()
        .reset_index()
        .rename(columns={"daily_return_pct": "avg_daily_return"})
        .sort_values("avg_daily_return", ascending=False)
    )
    avg_ret["color"] = avg_ret["avg_daily_return"].apply(
        lambda x: C["green"] if x >= 0 else C["red"]
    )

    fig_ret = go.Figure(go.Bar(
        x=avg_ret["avg_daily_return"],
        y=avg_ret["ticker_symbol"],
        orientation="h",
        marker_color=avg_ret["color"],
        text=avg_ret["avg_daily_return"].apply(lambda x: f"{x:+.3f}%"),
        textposition="outside",
        textfont=dict(size=9),
        hovertemplate="<b>%{y}</b><br>%{x:+.3f}%<extra></extra>",
    ))
    fig_ret.update_layout(**BASE_LAYOUT, height=360)
    fig_ret.update_xaxes(
        ticksuffix="%",
        zeroline=True,
        zerolinecolor=C["muted"],
        zerolinewidth=1,
    )
    st.plotly_chart(fig_ret, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# CHART 5 — INDIVIDUAL TICKER DEEP DIVE
# Lets the user select a single ticker and see its full price history,
# daily return distribution as a histogram, and a summary stats table.
# This is the most interactive section — it shows off Streamlit's strength
# as a tool for exploratory, on-demand analysis.
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("<div class='sec'>Ticker Deep Dive</div>", unsafe_allow_html=True)

selected_ticker = st.selectbox(
    "Select a ticker to inspect",
    options=sorted(df["ticker_symbol"].unique()),
    index=0,
)

ticker_df = df[df["ticker_symbol"] == selected_ticker].sort_values("trade_date")
fund_meta = ticker_df.iloc[0]

st.markdown(
    f"<span style='font-size:0.65rem;color:#4b5563'>"
    f"<b style='color:#94a3b8'>{fund_meta['fund_name']}</b>"
    f"&nbsp;&nbsp;·&nbsp;&nbsp;{fund_meta['sector']}"
    f"&nbsp;&nbsp;·&nbsp;&nbsp;{fund_meta['instrument_type'].replace('_', ' ').title()}"
    f"</span>",
    unsafe_allow_html=True,
)

# Price history
fig_drill = go.Figure()
fig_drill.add_trace(go.Scatter(
    x=ticker_df["trade_date"],
    y=ticker_df["close_price"],
    mode="lines",
    name="Close Price",
    line=dict(color=C["blue"], width=2),
    fill="tozeroy",
    fillcolor="rgba(59,130,246,0.06)",
    hovertemplate="<b>%{x|%b %d, %Y}</b><br>$%{y:.2f}<extra></extra>",
))
fig_drill.update_layout(**BASE_LAYOUT, height=220, title=f"{selected_ticker} — Close Price")
fig_drill.update_yaxes(tickprefix="$")
st.plotly_chart(fig_drill, use_container_width=True)

# Daily return distribution histogram
fig_hist = go.Figure()
fig_hist.add_trace(go.Histogram(
    x=ticker_df["daily_return_pct"].dropna(),
    nbinsx=30,
    marker_color=C["blue"],
    opacity=0.75,
    hovertemplate="Return: %{x:.2f}%<br>Days: %{y}<extra></extra>",
))
fig_hist.update_layout(
    **BASE_LAYOUT,
    height=180,
    title=f"{selected_ticker} — Daily Return Distribution",
    bargap=0.05,
)
fig_hist.update_xaxes(ticksuffix="%")
st.plotly_chart(fig_hist, use_container_width=True)

# Summary stats table
st.markdown("<div class='sec'>Summary Statistics</div>", unsafe_allow_html=True)

summary = pd.DataFrame({
    "Metric": [
        "First Close", "Latest Close", "Total Return",
        "Avg Daily Return", "Best Day", "Worst Day", "Trading Days",
    ],
    "Value": [
        f"${ticker_df['close_price'].iloc[0]:.2f}",
        f"${ticker_df['close_price'].iloc[-1]:.2f}",
        f"{((ticker_df['close_price'].iloc[-1] - ticker_df['close_price'].iloc[0]) / ticker_df['close_price'].iloc[0] * 100):+.2f}%",
        f"{ticker_df['daily_return_pct'].mean():+.3f}%",
        f"{ticker_df['daily_return_pct'].max():+.3f}%",
        f"{ticker_df['daily_return_pct'].min():+.3f}%",
        str(len(ticker_df)),
    ],
})
st.dataframe(summary, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.markdown(
    f"<span style='font-size:0.6rem;color:#374151'>"
    f"Data source: Yahoo Finance via yfinance&nbsp;&nbsp;·&nbsp;&nbsp;"
    f"Pipeline: Bronze → Silver → Gold (DuckDB)&nbsp;&nbsp;·&nbsp;&nbsp;"
    f"Last processed: {str(freshness.get('last_processed', 'unknown'))[:19]}"
    f"</span>",
    unsafe_allow_html=True,
)