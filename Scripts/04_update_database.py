from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

SOURCE = "reddit"
SENTENCE_DIR = Path("data/sentences/reddit")
DB_PATH = Path("data/database/framescope.db")


def load_json(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def create_tables_if_missing(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS reddit_sentence_items (
            sentence_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            item_type TEXT,
            source TEXT DEFAULT 'reddit',
            subreddit TEXT,
            author TEXT,
            created_utc INTEGER,
            sentence_index INTEGER,
            preceding_sentence TEXT,
            ai_sentence TEXT,
            subsequent_sentence TEXT,
            context_text TEXT,
            full_text TEXT,
            score INTEGER,
            num_comments INTEGER,
            url TEXT,
            link_id TEXT,
            parent_id TEXT,
            run_folder TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS llm_labels (
            source TEXT NOT NULL,
            sentence_id TEXT NOT NULL,
            metaphor_category TEXT,
            metaphor_present INTEGER,
            stance TEXT,
            confidence REAL,
            reasoning TEXT,
            model_name TEXT,
            labeled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source, sentence_id)
        );

        CREATE TABLE IF NOT EXISTS weekly_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            week_start TEXT,
            community TEXT,
            item_type TEXT,
            metaphor_category TEXT,
            stance TEXT,
            n_items INTEGER,
            avg_confidence REAL,
            avg_score REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS monthly_llm_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            month TEXT,
            summary_text TEXT,
            dominant_metaphors TEXT,
            dominant_stance TEXT,
            notable_shift TEXT,
            example_count INTEGER,
            model_name TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS example_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            sentence_id TEXT,
            community TEXT,
            created_utc INTEGER,
            ai_sentence TEXT,
            context_text TEXT,
            metaphor_category TEXT,
            stance TEXT,
            score INTEGER,
            selected_reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

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

        CREATE INDEX IF NOT EXISTS idx_reddit_sentence_items_run_folder
            ON reddit_sentence_items(run_folder);

        CREATE INDEX IF NOT EXISTS idx_reddit_sentence_items_subreddit
            ON reddit_sentence_items(subreddit);

        CREATE INDEX IF NOT EXISTS idx_reddit_sentence_items_created_utc
            ON reddit_sentence_items(created_utc);

        CREATE INDEX IF NOT EXISTS idx_llm_labels_source_sentence
            ON llm_labels(source, sentence_id);

        CREATE INDEX IF NOT EXISTS idx_llm_labels_metaphor
            ON llm_labels(source, metaphor_category);

        CREATE INDEX IF NOT EXISTS idx_llm_labels_stance
            ON llm_labels(source, stance);

        CREATE INDEX IF NOT EXISTS idx_weekly_summary_source
            ON weekly_summary(source);

        CREATE INDEX IF NOT EXISTS idx_example_items_source
            ON example_items(source);
        """
    )
    conn.commit()


def insert_reddit_sentence_items(
    conn: sqlite3.Connection,
    records: list[dict],
    run_folder: str,
) -> int:
    rows = []

    for r in records:
        sentence_id = r.get("sentence_id")
        item_id = r.get("item_id")

        if not sentence_id or not item_id:
            continue

        rows.append(
            (
                sentence_id,
                item_id,
                r.get("item_type"),
                SOURCE,
                r.get("subreddit"),
                r.get("author"),
                r.get("created_utc"),
                r.get("sentence_index"),
                r.get("preceding_sentence"),
                r.get("ai_sentence"),
                r.get("subsequent_sentence"),
                r.get("context_text"),
                r.get("full_text"),
                r.get("score"),
                r.get("num_comments"),
                r.get("url"),
                r.get("link_id"),
                r.get("parent_id"),
                run_folder,
            )
        )

    before = conn.total_changes

    conn.executemany(
        """
        INSERT OR IGNORE INTO reddit_sentence_items (
            sentence_id,
            item_id,
            item_type,
            source,
            subreddit,
            author,
            created_utc,
            sentence_index,
            preceding_sentence,
            ai_sentence,
            subsequent_sentence,
            context_text,
            full_text,
            score,
            num_comments,
            url,
            link_id,
            parent_id,
            run_folder
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )

    conn.commit()
    return conn.total_changes - before


def log_pipeline_run(
    conn: sqlite3.Connection,
    run_folder: str,
    stage: str,
    n_records: int,
    status: str,
    message: str,
) -> None:
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
        (SOURCE, run_folder, stage, n_records, status, message),
    )
    conn.commit()


def main() -> None:
    if not SENTENCE_DIR.exists():
        raise FileNotFoundError(
            "No Reddit sentence data found. Run Scripts/03_sentence_preprocess.py first."
        )

    conn = connect_db(DB_PATH)
    create_tables_if_missing(conn)

    total_new_records = 0
    folders_seen = 0

    for run_folder in sorted(SENTENCE_DIR.iterdir()):
        if not run_folder.is_dir():
            continue

        input_path = run_folder / "sentences.json"

        if not input_path.exists():
            logging.warning(
                "Skipping %s because sentences.json is missing",
                run_folder.name,
            )
            continue

        records = load_json(input_path)
        inserted = insert_reddit_sentence_items(conn, records, run_folder.name)

        total_new_records += inserted
        folders_seen += 1

        log_pipeline_run(
            conn=conn,
            run_folder=run_folder.name,
            stage="update_database",
            n_records=inserted,
            status="success",
            message=(
                f"Inserted {inserted} new Reddit sentence records; "
                "skipped existing duplicates automatically."
            ),
        )

        logging.info(
            "Processed %s | input_records=%s | new_records=%s",
            run_folder.name,
            len(records),
            inserted,
        )

    logging.info(
        "Reddit database update complete | folders_seen=%s | new_records=%s | db=%s",
        folders_seen,
        total_new_records,
        DB_PATH,
    )

    conn.close()


if __name__ == "__main__":
    main()