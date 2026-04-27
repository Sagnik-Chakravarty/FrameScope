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

        CREATE TABLE IF NOT EXISTS weekly_llm_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT 'reddit',
            week_start TEXT NOT NULL,
            week_end TEXT NOT NULL,
            scope TEXT NOT NULL,
            scope_value TEXT NOT NULL,
            granularity TEXT,
            stance_focus TEXT,
            summary_text TEXT NOT NULL,
            likely_drivers TEXT,
            dominant_metaphors TEXT,
            dominant_granularity TEXT,
            dominant_stance TEXT,
            evidence_count INTEGER,
            example_sentence_ids TEXT,
            model_name TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(
                source,
                week_start,
                week_end,
                scope,
                scope_value,
                granularity,
                stance_focus
            )
        );

        CREATE TABLE IF NOT EXISTS monthly_llm_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT 'reddit',
            month TEXT NOT NULL,
            scope TEXT NOT NULL,
            scope_value TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            likely_drivers TEXT,
            dominant_metaphors TEXT,
            dominant_granularity TEXT,
            dominant_stance TEXT,
            weeks_covered INTEGER,
            weekly_summary_ids TEXT,
            model_name TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, month, scope, scope_value)
        );

        CREATE TABLE IF NOT EXISTS yearly_llm_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT 'reddit',
            year TEXT NOT NULL,
            scope TEXT NOT NULL,
            scope_value TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            likely_drivers TEXT,
            dominant_metaphors TEXT,
            dominant_granularity TEXT,
            dominant_stance TEXT,
            months_covered INTEGER,
            monthly_summary_ids TEXT,
            model_name TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, year, scope, scope_value)
        );

        CREATE TABLE IF NOT EXISTS volume_shift_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT 'reddit',
            period_type TEXT NOT NULL,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            scope TEXT NOT NULL,
            scope_value TEXT NOT NULL,
            shift_summary TEXT NOT NULL,
            key_transitions TEXT,
            volume_change TEXT,
            stance_shift TEXT,
            metaphor_shift TEXT,
            evidence_count INTEGER,
            comparison_period TEXT,
            model_name TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, period_type, period_start, scope, scope_value)
        );

        CREATE TABLE IF NOT EXISTS aggregate_weekly_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            week_start TEXT NOT NULL,
            week_end TEXT NOT NULL,
            subreddit TEXT,
            item_type TEXT,
            metaphor_category TEXT,
            granularity TEXT,
            stance TEXT,
            n_sentences INTEGER,
            n_items INTEGER,
            avg_score REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS polarizing_examples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            period_type TEXT NOT NULL,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            scope TEXT NOT NULL,
            scope_value TEXT,
            subreddit TEXT,
            item_type TEXT,
            metaphor_category TEXT,
            granularity TEXT,
            stance TEXT,
            sentence_id TEXT,
            post_id TEXT,
            context_text TEXT,
            ai_sentence TEXT,
            score INTEGER,
            rank INTEGER,
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

        CREATE INDEX IF NOT EXISTS idx_weekly_llm_summary_period
            ON weekly_llm_summary(source, week_start, week_end);

        CREATE INDEX IF NOT EXISTS idx_weekly_llm_summary_scope
            ON weekly_llm_summary(source, scope, scope_value);

        CREATE INDEX IF NOT EXISTS idx_monthly_llm_summary_period
            ON monthly_llm_summary(source, month);

        CREATE INDEX IF NOT EXISTS idx_monthly_llm_summary_scope
            ON monthly_llm_summary(source, scope, scope_value);

        CREATE INDEX IF NOT EXISTS idx_yearly_llm_summary_period
            ON yearly_llm_summary(source, year);

        CREATE INDEX IF NOT EXISTS idx_yearly_llm_summary_scope
            ON yearly_llm_summary(source, scope, scope_value);

        CREATE INDEX IF NOT EXISTS idx_shift_period
            ON volume_shift_summary(source, period_type, period_start);

        CREATE INDEX IF NOT EXISTS idx_shift_scope
            ON volume_shift_summary(source, scope, scope_value);

        CREATE INDEX IF NOT EXISTS idx_aggregate_weekly_period
            ON aggregate_weekly_metrics(source, week_start, week_end);

        CREATE INDEX IF NOT EXISTS idx_aggregate_weekly_group
            ON aggregate_weekly_metrics(
                source,
                subreddit,
                metaphor_category,
                granularity,
                stance
            );

        CREATE INDEX IF NOT EXISTS idx_polarizing_examples_period
            ON polarizing_examples(source, period_type, period_start, period_end);

        CREATE INDEX IF NOT EXISTS idx_polarizing_examples_scope
            ON polarizing_examples(source, scope, scope_value);

        CREATE INDEX IF NOT EXISTS idx_polarizing_examples_group
            ON polarizing_examples(
                source,
                subreddit,
                metaphor_category,
                granularity,
                stance
            );
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