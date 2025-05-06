# src/06_push_to_db.py
"""
Append new Lassa rows to an online Postgres table.

Env vars needed:
    DATABASE_URL = "postgresql+psycopg2://user:pass@host:port/dbname"
"""
import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

def main():
    csv_path = max(Path("data/processed").glob("combined_lassa_data_*.csv"))
    df = pd.read_csv(csv_path)

    engine = create_engine(os.environ["DATABASE_URL"])
    with engine.begin() as conn:
        # Simple append; use ON CONFLICT if you want upsert
        df.to_sql("lassa_reports", conn, if_exists="append", index=False, method="multi")

    logging.info("Uploaded %d rows from %s", len(df), csv_path)

if __name__ == "__main__":
    main()