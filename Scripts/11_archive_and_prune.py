from __future__ import annotations

import argparse
import logging
import shutil
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROTECTED_DIRS = {"model_eval"}
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

SOURCE_DB = Path("data/database/framescope.db")
ARCHIVE_ROOT = Path("archive")
AGGREGATE_DB_DIR = Path("data/aggregate")
AGGREGATE_DB = AGGREGATE_DB_DIR / "framescope_aggregate.db"

DATA_DIRS_TO_DELETE = [
    Path("data/raw"),
    Path("data/processed"),
    Path("data/cleaned"),
    Path("data/sentences"),
]

DASHBOARD_TABLES = [
    "aggregate_weekly_metrics",
    "polarizing_examples",
    "weekly_llm_summary",
    "monthly_llm_summary",
    "yearly_llm_summary",
    "volume_shift_summary",
    "pipeline_runs",
]


def get_week_label(today: datetime | None = None) -> str:
    if today is None:
        today = datetime.now(timezone.utc)

    iso_year, iso_week, _ = today.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def get_archive_dir(week_label: str) -> Path:
    return ARCHIVE_ROOT / week_label


def copy_full_database_to_archive(source_db: Path, archive_dir: Path) -> Path:
    if not source_db.exists():
        raise FileNotFoundError(f"Source database not found: {source_db}")

    archive_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    archive_db = archive_dir / f"framescope_full_{timestamp}.db"

    shutil.copy2(source_db, archive_db)

    wal_file = Path(str(source_db) + "-wal")
    shm_file = Path(str(source_db) + "-shm")

    if wal_file.exists():
        shutil.copy2(wal_file, archive_dir / f"framescope_full_{timestamp}.db-wal")

    if shm_file.exists():
        shutil.copy2(shm_file, archive_dir / f"framescope_full_{timestamp}.db-shm")

    logging.info("Archived full database to %s", archive_db)
    return archive_db


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
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


def copy_table(source_conn: sqlite3.Connection, dest_conn: sqlite3.Connection, table: str) -> int:
    if not table_exists(source_conn, table):
        logging.warning("Skipping missing table: %s", table)
        return 0

    create_sql_row = source_conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?;
        """,
        (table,),
    ).fetchone()

    if create_sql_row is None or create_sql_row[0] is None:
        logging.warning("Could not find CREATE SQL for table: %s", table)
        return 0

    dest_conn.execute(f"DROP TABLE IF EXISTS {table};")
    dest_conn.execute(create_sql_row[0])

    columns = [
        row[1]
        for row in source_conn.execute(f"PRAGMA table_info({table});").fetchall()
    ]

    col_sql = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))

    rows = source_conn.execute(f"SELECT {col_sql} FROM {table};").fetchall()

    if rows:
        dest_conn.executemany(
            f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders});",
            rows,
        )

    dest_conn.commit()

    logging.info("Copied table %s | rows=%s", table, len(rows))
    return len(rows)


def copy_indexes(source_conn: sqlite3.Connection, dest_conn: sqlite3.Connection) -> None:
    rows = source_conn.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'index'
          AND sql IS NOT NULL;
        """
    ).fetchall()

    for name, sql in rows:
        if not sql:
            continue

        if any(table in sql for table in DASHBOARD_TABLES):
            try:
                dest_conn.execute(sql)
            except sqlite3.OperationalError:
                pass

    dest_conn.commit()


def create_aggregate_database(source_db: Path, aggregate_db: Path) -> None:
    aggregate_db.parent.mkdir(parents=True, exist_ok=True)

    if aggregate_db.exists():
        aggregate_db.unlink()

    source_conn = sqlite3.connect(source_db)
    dest_conn = sqlite3.connect(aggregate_db)

    try:
        source_conn.row_factory = sqlite3.Row

        total_rows = 0

        for table in DASHBOARD_TABLES:
            total_rows += copy_table(source_conn, dest_conn, table)

        copy_indexes(source_conn, dest_conn)

        dest_conn.execute("VACUUM;")
        dest_conn.commit()

        logging.info(
            "Created aggregate dashboard DB at %s | copied_rows=%s",
            aggregate_db,
            total_rows,
        )

    finally:
        source_conn.close()
        dest_conn.close()


def delete_data_directories(dry_run: bool) -> None:
    for path in DATA_DIRS_TO_DELETE:
        if not path.exists():
            logging.info("Skipping missing path: %s", path)
            continue

        # Iterate inside directory instead of deleting whole folder
        for item in path.iterdir():
            if item.name in PROTECTED_DIRS:
                logging.info("Skipping protected folder: %s", item)
                continue

            if dry_run:
                logging.info("[DRY RUN] Would delete %s", item)
            else:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()

                logging.info("Deleted %s", item)


def log_archive_run(source_db: Path, archive_db: Path, aggregate_db: Path) -> None:
    conn = sqlite3.connect(source_db)

    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                run_folder TEXT,
                stage TEXT,
                n_records INTEGER,
                status TEXT,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        conn.execute(
            """
            INSERT INTO pipeline_runs (
                source,
                run_folder,
                stage,
                n_records,
                status,
                message
            )
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                "reddit",
                None,
                "archive_and_prune",
                None,
                "success",
                f"Archived full DB to {archive_db}; created aggregate DB at {aggregate_db}.",
            ),
        )

        conn.commit()

    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Archive full FrameScope DB, create dashboard aggregate DB, and prune temporary data folders."
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without deleting files.",
    )

    parser.add_argument(
        "--week-label",
        type=str,
        default=None,
        help="Archive folder label, e.g. 2026-W17. Defaults to current ISO week.",
    )

    parser.add_argument(
        "--skip-delete",
        action="store_true",
        help="Create archive and aggregate DB but do not delete data folders.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    week_label = args.week_label or get_week_label()
    archive_dir = get_archive_dir(week_label)

    logging.info("Starting archive/prune step | week_label=%s", week_label)

    archive_db = copy_full_database_to_archive(SOURCE_DB, archive_dir)

    create_aggregate_database(
        source_db=SOURCE_DB,
        aggregate_db=AGGREGATE_DB,
    )

    log_archive_run(
        source_db=SOURCE_DB,
        archive_db=archive_db,
        aggregate_db=AGGREGATE_DB,
    )

    if args.skip_delete:
        logging.info("Skipping deletion because --skip-delete was used.")
    else:
        delete_data_directories(dry_run=args.dry_run)

    logging.info("Archive/prune complete.")


if __name__ == "__main__":
    main()