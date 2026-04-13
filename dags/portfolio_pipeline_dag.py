"""
401k Portfolio Pipeline DAG
=============================
PYTHON = "/home/administrator/airflow-venv/bin/python"Runs the full Bronze → Silver → Gold pipeline daily at 6pm.
Each task runs the corresponding pipeline script via BashOperator.
If any task fails, downstream tasks are skipped automatically.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ── Default arguments inherited by every task in the DAG ──────────────────────
# retries=1 means if a task fails, Airflow will try it one more time
# retry_delay=5 minutes means it waits 5 minutes before retrying
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_DIR = "/mnt/a/Projects/401k-portfolio-tracker"
PYTHON = "/home/administrator/airflow-venv/bin/python"

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="portfolio_pipeline",
    description="Daily Bronze → Silver → Gold ELT pipeline for 401k portfolio",
    default_args=default_args,
    start_date=datetime(2025, 3, 9),
    schedule_interval="0 18 * * 1-5",  # 6pm Monday–Friday (market days only)
    catchup=False,                      # don't backfill missed runs
    tags=["portfolio", "etl"],
) as dag:

    # Task 1 — Bronze
    # Extracts raw daily prices from Yahoo Finance → Parquet
    bronze = BashOperator(
        task_id="bronze_extraction",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} pipelines/bronze/bronze_stock_prices.py",
    )

    # Task 2 — Silver
    # Cleans and incrementally loads Bronze → Silver Parquet
    silver = BashOperator(
        task_id="silver_transformation",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} pipelines/silver/silver_stock_prices.py",
    )

    # Task 3 — Gold
    # Builds star schema in DuckDB from Silver
    gold = BashOperator(
        task_id="gold_modeling",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} pipelines/gold/gold_stock_prices.py",
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    # This is the DAG structure — Bronze must succeed before Silver runs,
    # Silver must succeed before Gold runs. The >> operator sets this order.
    bronze >> silver >> gold
