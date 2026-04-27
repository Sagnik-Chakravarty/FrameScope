from __future__ import annotations

import argparse
import logging
import os
import sqlite3
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

SQLITE_DB = Path("data/aggregate/framescope_aggregate.db")
ENV_PATH = Path(".env")
NEON_ENV_KEY = "NeonDb"

DASHBOARD_TABLES = [
    "aggregate_weekly_metrics",
    "polarizing_examples",
    "weekly_llm_summary",
    "monthly_llm_summary",
    "yearly_llm_summary",
    "volume_shift_summary",
    "pipeline_runs",
]


def load_neon_url() -> str:
    load_dotenv(ENV_PATH)

    neon_url = os.getenv(NEON_ENV_KEY)

    if not neon_url:
        raise ValueError(
            f"Missing Neon connection string. Add this to .env:\n\n"
            f"{NEON_ENV_KEY}=postgresql://USER:PASSWORD@HOST/dbname?sslmode=require"
        )

    if neon_url.startswith("postgres://"):
        neon_url = neon_url.replace("postgres://", "postgresql://", 1)

    return neon_url


def check_sqlite_db(sqlite_db: Path) -> None:
    if not sqlite_db.exists():
        raise FileNotFoundError(f"Aggregate SQLite DB not found: {sqlite_db}")


def sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        LIMIT 1;
        """,
        (table,),
    ).fetchone()

    return row is not None


def get_existing_tables(sqlite_db: Path) -> list[str]:
    conn = sqlite3.connect(sqlite_db)

    try:
        existing = []

        for table in DASHBOARD_TABLES:
            if sqlite_table_exists(conn, table):
                existing.append(table)
            else:
                logging.warning("Skipping missing SQLite table: %s", table)

        return existing

    finally:
        conn.close()


def read_sqlite_table(sqlite_db: Path, table: str) -> pd.DataFrame:
    conn = sqlite3.connect(sqlite_db)

    try:
        return pd.read_sql_query(f'SELECT * FROM "{table}";', conn)

    finally:
        conn.close()


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].where(df[col].notna(), None)

    return df


def postgres_table_exists(engine, table: str) -> bool:
    query = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = :table_name
        ) AS exists;
        """
    )

    with engine.begin() as conn:
        return bool(conn.execute(query, {"table_name": table}).scalar())


def clear_postgres_table(engine, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'DELETE FROM "{table}";'))


def upload_table_sync(
    engine,
    table: str,
    df: pd.DataFrame,
    chunksize: int,
) -> int:
    df = normalize_dataframe(df)

    logging.info("Syncing table=%s | rows=%s", table, len(df))

    if postgres_table_exists(engine, table):
        logging.info("Clearing existing rows from Neon table: %s", table)
        clear_postgres_table(engine, table)
        if_exists = "append"
    else:
        logging.info("Creating Neon table because it does not exist: %s", table)
        if_exists = "fail"

    df.to_sql(
        name=table,
        con=engine,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
        method="multi",
    )

    return len(df)


def create_indexes(engine) -> None:
    index_statements = [
        """
        CREATE INDEX IF NOT EXISTS idx_neon_aggregate_weekly_period
        ON aggregate_weekly_metrics(source, week_start, week_end);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_aggregate_weekly_group
        ON aggregate_weekly_metrics(source, subreddit, metaphor_category, granularity, stance);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_polarizing_period
        ON polarizing_examples(source, period_type, period_start, period_end);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_polarizing_group
        ON polarizing_examples(source, subreddit, metaphor_category, granularity, stance);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_weekly_summary_period
        ON weekly_llm_summary(source, week_start, week_end);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_weekly_summary_scope
        ON weekly_llm_summary(source, scope, scope_value);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_monthly_summary_period
        ON monthly_llm_summary(source, month);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_monthly_summary_scope
        ON monthly_llm_summary(source, scope, scope_value);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_yearly_summary_period
        ON yearly_llm_summary(source, year);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_yearly_summary_scope
        ON yearly_llm_summary(source, scope, scope_value);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_neon_volume_shift_period
        ON volume_shift_summary(source, period_type, period_start);
        """,
    ]

    with engine.begin() as conn:
        for stmt in index_statements:
            conn.execute(text(stmt))

    logging.info("Postgres indexes created/verified.")


def log_upload_metadata(engine, uploaded_rows: dict[str, int]) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS neon_upload_runs (
                    id SERIAL PRIMARY KEY,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    uploaded_tables TEXT,
                    total_rows INTEGER,
                    status TEXT
                );
                """
            )
        )

        conn.execute(
            text(
                """
                INSERT INTO neon_upload_runs (
                    uploaded_tables,
                    total_rows,
                    status
                )
                VALUES (
                    :uploaded_tables,
                    :total_rows,
                    :status
                );
                """
            ),
            {
                "uploaded_tables": ", ".join(uploaded_rows.keys()),
                "total_rows": sum(uploaded_rows.values()),
                "status": "success",
            },
        )


def upload_to_neon(
    sqlite_db: Path,
    neon_url: str,
    chunksize: int,
) -> None:
    check_sqlite_db(sqlite_db)

    tables = get_existing_tables(sqlite_db)

    if not tables:
        raise RuntimeError("No dashboard tables found in aggregate SQLite DB.")

    engine = create_engine(neon_url, pool_pre_ping=True)

    uploaded_rows: dict[str, int] = {}

    try:
        for table in tables:
            df = read_sqlite_table(sqlite_db, table)

            row_count = upload_table_sync(
                engine=engine,
                table=table,
                df=df,
                chunksize=chunksize,
            )

            uploaded_rows[table] = row_count

        create_indexes(engine)
        log_upload_metadata(engine, uploaded_rows)

    finally:
        engine.dispose()

    logging.info("Upload complete | total_rows=%s", sum(uploaded_rows.values()))

    for table, n in uploaded_rows.items():
        logging.info("  %s: %s rows", table, n)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync FrameScope aggregate SQLite database to Neon Postgres."
    )

    parser.add_argument(
        "--sqlite-db",
        type=str,
        default=str(SQLITE_DB),
        help="Path to aggregate SQLite DB.",
    )

    parser.add_argument(
        "--chunksize",
        type=int,
        default=5000,
        help="Rows per upload batch.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    neon_url = load_neon_url()

    upload_to_neon(
        sqlite_db=Path(args.sqlite_db),
        neon_url=neon_url,
        chunksize=args.chunksize,
    )

    print("\nDone. Aggregate database synced to Neon.\n")


if __name__ == "__main__":
    main()