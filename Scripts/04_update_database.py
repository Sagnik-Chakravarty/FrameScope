from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

SOURCE = "reddit"
SENTENCE_DIR = Path("data/sentences/reddit")
DB_PATH = Path("data/database/framescope.db")


def load_json(path: Path) -> list[dict[str, Any]]:
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
        CREATE TABLE IF NOT EXISTS reddit_posts (
            source TEXT NOT NULL DEFAULT 'reddit',
            post_id TEXT NOT NULL,
            item_type TEXT,
            subreddit TEXT,
            author TEXT,
            created_utc INTEGER,
            created_datetime TEXT,
            title TEXT,
            selftext TEXT,
            text TEXT,
            score INTEGER,
            num_comments INTEGER,
            url TEXT,
            link_id TEXT,
            parent_id TEXT,
            raw_file TEXT,
            run_folder TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source, post_id)
        );

        CREATE TABLE IF NOT EXISTS reddit_sentence_items (
            source TEXT NOT NULL DEFAULT 'reddit',
            sentence_id TEXT NOT NULL,
            post_id TEXT NOT NULL,
            item_type TEXT,
            subreddit TEXT,
            author TEXT,
            created_utc INTEGER,
            created_datetime TEXT,
            sentence_index INTEGER,
            preceding_sentence TEXT,
            ai_sentence TEXT NOT NULL,
            subsequent_sentence TEXT,
            context_text TEXT,
            full_text TEXT,
            score INTEGER,
            num_comments INTEGER,
            url TEXT,
            link_id TEXT,
            parent_id TEXT,
            raw_file TEXT,
            run_folder TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source, sentence_id),
            FOREIGN KEY (source, post_id)
                REFERENCES reddit_posts(source, post_id)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS llm_labels (
            source TEXT NOT NULL,
            sentence_id TEXT NOT NULL,
            metaphor_category TEXT,
            metaphor_present INTEGER,
            granularity TEXT,
            stance TEXT,
            confidence REAL,
            reasoning TEXT,
            model_name TEXT,
            labeled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source, sentence_id),
            FOREIGN KEY (source, sentence_id)
                REFERENCES reddit_sentence_items(source, sentence_id)
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS weekly_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            week_start TEXT,
            community TEXT,
            item_type TEXT,
            metaphor_category TEXT,
            granularity TEXT,
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
            dominant_granularity TEXT,
            dominant_stance TEXT,
            notable_shift TEXT,
            example_count INTEGER,
            model_name TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

        CREATE INDEX IF NOT EXISTS idx_reddit_posts_subreddit
            ON reddit_posts(source, subreddit);

        CREATE INDEX IF NOT EXISTS idx_reddit_posts_created_utc
            ON reddit_posts(source, created_utc);

        CREATE INDEX IF NOT EXISTS idx_reddit_posts_run_folder
            ON reddit_posts(source, run_folder);

        CREATE INDEX IF NOT EXISTS idx_sentence_items_post_id
            ON reddit_sentence_items(source, post_id);

        CREATE INDEX IF NOT EXISTS idx_sentence_items_subreddit
            ON reddit_sentence_items(source, subreddit);

        CREATE INDEX IF NOT EXISTS idx_sentence_items_created_utc
            ON reddit_sentence_items(source, created_utc);

        CREATE INDEX IF NOT EXISTS idx_sentence_items_run_folder
            ON reddit_sentence_items(source, run_folder);

        CREATE INDEX IF NOT EXISTS idx_llm_labels_metaphor
            ON llm_labels(source, metaphor_category);

        CREATE INDEX IF NOT EXISTS idx_llm_labels_granularity
            ON llm_labels(source, granularity);

        CREATE INDEX IF NOT EXISTS idx_llm_labels_stance
            ON llm_labels(source, stance);

        CREATE INDEX IF NOT EXISTS idx_weekly_summary_source
            ON weekly_summary(source, week_start);

        CREATE INDEX IF NOT EXISTS idx_monthly_summary_source
            ON monthly_llm_summary(source, month);
        """
    )

    conn.commit()


def get_post_id(record: dict[str, Any]) -> str | None:
    item_id = record.get("item_id")

    if not item_id:
        return None

    return str(item_id)


def insert_reddit_posts(
    conn: sqlite3.Connection,
    records: list[dict[str, Any]],
    run_folder: str,
) -> int:
    rows = []
    seen = set()

    for r in records:
        post_id = get_post_id(r)

        if not post_id:
            continue

        key = (SOURCE, post_id)

        if key in seen:
            continue

        seen.add(key)

        full_text = r.get("full_text")
        context_text = r.get("context_text")
        ai_sentence = r.get("ai_sentence")

        rows.append(
            (
                SOURCE,
                post_id,
                r.get("item_type"),
                r.get("subreddit"),
                r.get("author"),
                r.get("created_utc"),
                r.get("created_datetime"),
                r.get("title"),
                r.get("selftext"),
                full_text or context_text or ai_sentence,
                r.get("score"),
                r.get("num_comments"),
                r.get("url"),
                r.get("link_id"),
                r.get("parent_id"),
                str(r.get("raw_file") or ""),
                run_folder,
            )
        )

    before = conn.total_changes

    conn.executemany(
        """
        INSERT OR IGNORE INTO reddit_posts (
            source,
            post_id,
            item_type,
            subreddit,
            author,
            created_utc,
            created_datetime,
            title,
            selftext,
            text,
            score,
            num_comments,
            url,
            link_id,
            parent_id,
            raw_file,
            run_folder
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )

    conn.commit()

    return conn.total_changes - before


def insert_reddit_sentence_items(
    conn: sqlite3.Connection,
    records: list[dict[str, Any]],
    run_folder: str,
) -> int:
    rows = []

    for r in records:
        sentence_id = r.get("sentence_id")
        post_id = get_post_id(r)

        if not sentence_id or not post_id:
            continue

        ai_sentence = r.get("ai_sentence")

        if not ai_sentence:
            continue

        rows.append(
            (
                SOURCE,
                str(sentence_id),
                post_id,
                r.get("item_type"),
                r.get("subreddit"),
                r.get("author"),
                r.get("created_utc"),
                r.get("created_datetime"),
                r.get("sentence_index"),
                r.get("preceding_sentence"),
                ai_sentence,
                r.get("subsequent_sentence"),
                r.get("context_text"),
                r.get("full_text"),
                r.get("score"),
                r.get("num_comments"),
                r.get("url"),
                r.get("link_id"),
                r.get("parent_id"),
                str(r.get("raw_file") or ""),
                run_folder,
            )
        )

    before = conn.total_changes

    conn.executemany(
        """
        INSERT OR IGNORE INTO reddit_sentence_items (
            source,
            sentence_id,
            post_id,
            item_type,
            subreddit,
            author,
            created_utc,
            created_datetime,
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
            raw_file,
            run_folder
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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


def audit_database(conn: sqlite3.Connection) -> None:
    tables = [
        "reddit_posts",
        "reddit_sentence_items",
        "llm_labels",
        "weekly_summary",
        "monthly_llm_summary",
        "pipeline_runs",
    ]

    logging.info("Database audit:")

    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        logging.info("  %s: %s rows", table, count)


def main() -> None:
    if not SENTENCE_DIR.exists():
        raise FileNotFoundError(
            "No Reddit sentence data found. Run scripts/03_sentence_preprocess.py first."
        )

    conn = connect_db(DB_PATH)
    create_tables_if_missing(conn)

    total_new_posts = 0
    total_new_sentences = 0
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

        new_posts = insert_reddit_posts(
            conn=conn,
            records=records,
            run_folder=run_folder.name,
        )

        new_sentences = insert_reddit_sentence_items(
            conn=conn,
            records=records,
            run_folder=run_folder.name,
        )

        total_new_posts += new_posts
        total_new_sentences += new_sentences
        folders_seen += 1

        log_pipeline_run(
            conn=conn,
            run_folder=run_folder.name,
            stage="update_database",
            n_records=new_sentences,
            status="success",
            message=(
                f"Inserted {new_posts} new Reddit post/item records and "
                f"{new_sentences} new sentence records; duplicates skipped."
            ),
        )

        logging.info(
            "Processed %s | input_records=%s | new_posts=%s | new_sentences=%s",
            run_folder.name,
            len(records),
            new_posts,
            new_sentences,
        )

    logging.info(
        "Reddit database update complete | folders_seen=%s | new_posts=%s | new_sentences=%s | db=%s",
        folders_seen,
        total_new_posts,
        total_new_sentences,
        DB_PATH,
    )

    audit_database(conn)
    conn.close()


if __name__ == "__main__":
    main()