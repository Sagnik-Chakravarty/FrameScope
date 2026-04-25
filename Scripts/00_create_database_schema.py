from __future__ import annotations

import sqlite3
from pathlib import Path


DB_PATH = Path("data/database/framescope.db")


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS reddit_posts (
            source TEXT NOT NULL DEFAULT 'reddit',
            post_id TEXT NOT NULL,
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
            permalink TEXT,
            raw_file TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source, post_id)
        );

        CREATE TABLE IF NOT EXISTS reddit_sentence_items (
            source TEXT NOT NULL DEFAULT 'reddit',
            sentence_id TEXT NOT NULL,
            post_id TEXT NOT NULL,
            subreddit TEXT,
            created_utc INTEGER,
            created_datetime TEXT,
            preceding_sentence TEXT,
            ai_sentence TEXT NOT NULL,
            subsequent_sentence TEXT,
            context_text TEXT,
            score INTEGER,
            raw_file TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source, sentence_id),
            FOREIGN KEY (source, post_id)
                REFERENCES reddit_posts (source, post_id)
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
                REFERENCES reddit_sentence_items (source, sentence_id)
                ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_reddit_posts_subreddit
            ON reddit_posts (subreddit);

        CREATE INDEX IF NOT EXISTS idx_reddit_posts_created_utc
            ON reddit_posts (created_utc);

        CREATE INDEX IF NOT EXISTS idx_sentence_items_post_id
            ON reddit_sentence_items (post_id);

        CREATE INDEX IF NOT EXISTS idx_sentence_items_subreddit
            ON reddit_sentence_items (subreddit);

        CREATE INDEX IF NOT EXISTS idx_sentence_items_created_utc
            ON reddit_sentence_items (created_utc);

        CREATE INDEX IF NOT EXISTS idx_llm_labels_metaphor
            ON llm_labels (metaphor_category);

        CREATE INDEX IF NOT EXISTS idx_llm_labels_stance
            ON llm_labels (stance);

        CREATE INDEX IF NOT EXISTS idx_llm_labels_granularity
            ON llm_labels (granularity);
        """
    )

    conn.commit()


def main() -> None:
    conn = connect_db(DB_PATH)
    create_schema(conn)

    tables = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        ORDER BY name;
        """
    ).fetchall()

    print("Database schema created at:", DB_PATH)
    print("Tables:")
    for table in tables:
        print(" -", table[0])

    conn.close()


if __name__ == "__main__":
    main()