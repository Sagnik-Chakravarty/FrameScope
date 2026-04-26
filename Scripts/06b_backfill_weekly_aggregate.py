from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tqdm import tqdm


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

SOURCE = "reddit"
DB_PATH = Path("data/database/framescope.db")


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.row_factory = sqlite3.Row
    return conn


def create_aggregate_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
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


def unix_timestamp(dt: datetime) -> int:
    return int(dt.timestamp())


def parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def ts_to_utc(ts: int) -> datetime:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def floor_to_monday(dt: datetime) -> datetime:
    dt_midnight = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return dt_midnight - timedelta(days=dt_midnight.weekday())


def ceil_to_next_monday(dt: datetime) -> datetime:
    monday = floor_to_monday(dt)
    if dt == monday:
        return monday
    return monday + timedelta(days=7)


def get_labeled_data_bounds(conn: sqlite3.Connection) -> tuple[datetime, datetime]:
    row = conn.execute(
        """
        SELECT
            MIN(r.created_utc) AS min_ts,
            MAX(r.created_utc) AS max_ts
        FROM reddit_sentence_items r
        INNER JOIN llm_labels l
            ON r.source = l.source
           AND r.sentence_id = l.sentence_id
        WHERE r.source = ?
          AND r.created_utc IS NOT NULL;
        """,
        (SOURCE,),
    ).fetchone()

    if row is None or row["min_ts"] is None or row["max_ts"] is None:
        raise RuntimeError(
            "No labeled Reddit sentence data found. Run 05_label_llm.py first."
        )

    min_dt = ts_to_utc(row["min_ts"])
    max_dt = ts_to_utc(row["max_ts"])

    start = floor_to_monday(min_dt)
    end = ceil_to_next_monday(max_dt)

    return start, end


def iter_weeks(start_dt: datetime, end_dt: datetime) -> list[tuple[datetime, datetime]]:
    weeks = []
    current = start_dt

    while current < end_dt:
        next_week = current + timedelta(days=7)
        weeks.append((current, next_week))
        current = next_week

    return weeks


def week_has_labeled_data(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM reddit_sentence_items r
        INNER JOIN llm_labels l
            ON r.source = l.source
           AND r.sentence_id = l.sentence_id
        WHERE r.source = ?
          AND r.created_utc >= ?
          AND r.created_utc < ?;
        """,
        (SOURCE, start_ts, end_ts),
    ).fetchone()

    return int(row["n"]) > 0


def week_already_aggregated(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM aggregate_weekly_metrics
        WHERE source = ?
          AND week_start = ?
          AND week_end = ?;
        """,
        (SOURCE, week_start, week_end),
    ).fetchone()

    return int(row["n"]) > 0


def delete_existing_week(conn: sqlite3.Connection, week_start: str, week_end: str) -> None:
    conn.execute(
        """
        DELETE FROM aggregate_weekly_metrics
        WHERE source = ?
          AND week_start = ?
          AND week_end = ?;
        """,
        (SOURCE, week_start, week_end),
    )

    conn.execute(
        """
        DELETE FROM polarizing_examples
        WHERE source = ?
          AND period_type = 'week'
          AND period_start = ?
          AND period_end = ?;
        """,
        (SOURCE, week_start, week_end),
    )

    conn.commit()


def insert_weekly_metrics(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
    start_ts: int,
    end_ts: int,
) -> int:
    before = conn.total_changes

    conn.execute(
        """
        INSERT INTO aggregate_weekly_metrics (
            source,
            week_start,
            week_end,
            subreddit,
            item_type,
            metaphor_category,
            granularity,
            stance,
            n_sentences,
            n_items,
            avg_score
        )
        SELECT
            r.source,
            ? AS week_start,
            ? AS week_end,
            COALESCE(r.subreddit, 'Unknown') AS subreddit,
            COALESCE(r.item_type, 'Unknown') AS item_type,
            COALESCE(l.metaphor_category, 'None') AS metaphor_category,
            COALESCE(l.granularity, 'Not Applicable') AS granularity,
            COALESCE(l.stance, 'Neutral/Unclear') AS stance,
            COUNT(*) AS n_sentences,
            COUNT(DISTINCT r.post_id) AS n_items,
            AVG(r.score) AS avg_score
        FROM reddit_sentence_items r
        INNER JOIN llm_labels l
            ON r.source = l.source
           AND r.sentence_id = l.sentence_id
        WHERE r.source = ?
          AND r.created_utc >= ?
          AND r.created_utc < ?
        GROUP BY
            r.source,
            COALESCE(r.subreddit, 'Unknown'),
            COALESCE(r.item_type, 'Unknown'),
            COALESCE(l.metaphor_category, 'None'),
            COALESCE(l.granularity, 'Not Applicable'),
            COALESCE(l.stance, 'Neutral/Unclear');
        """,
        (week_start, week_end, SOURCE, start_ts, end_ts),
    )

    conn.commit()
    return conn.total_changes - before


def insert_polarizing_examples_by_subreddit_group(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
    start_ts: int,
    end_ts: int,
    top_n: int,
) -> int:
    before = conn.total_changes

    conn.execute(
        """
        INSERT INTO polarizing_examples (
            source,
            period_type,
            period_start,
            period_end,
            scope,
            scope_value,
            subreddit,
            item_type,
            metaphor_category,
            granularity,
            stance,
            sentence_id,
            post_id,
            context_text,
            ai_sentence,
            score,
            rank
        )
        WITH sentence_level AS (
            SELECT
                r.source,
                COALESCE(r.subreddit, 'Unknown') AS subreddit,
                COALESCE(r.item_type, 'Unknown') AS item_type,
                COALESCE(l.metaphor_category, 'None') AS metaphor_category,
                COALESCE(l.granularity, 'Not Applicable') AS granularity,
                COALESCE(l.stance, 'Neutral/Unclear') AS stance,
                r.sentence_id,
                r.post_id,
                r.created_utc,
                COALESCE(r.score, 0) AS score,
                TRIM(
                    COALESCE(r.preceding_sentence, '') || ' ' ||
                    COALESCE(r.ai_sentence, '') || ' ' ||
                    COALESCE(r.subsequent_sentence, '')
                ) AS local_context,
                TRIM(COALESCE(r.ai_sentence, '')) AS ai_sentence
            FROM reddit_sentence_items r
            INNER JOIN llm_labels l
                ON r.source = l.source
               AND r.sentence_id = l.sentence_id
            WHERE r.source = ?
              AND r.created_utc >= ?
              AND r.created_utc < ?
              AND l.stance IN ('Positive', 'Negative')
        ),

        item_level AS (
            SELECT
                source,
                subreddit,
                item_type,
                metaphor_category,
                granularity,
                stance,
                post_id,
                GROUP_CONCAT(sentence_id, ' || ') AS sentence_id,
                GROUP_CONCAT(local_context, '\n\n---\n\n') AS context_text,
                GROUP_CONCAT(ai_sentence, '\n\n') AS ai_sentence,
                MAX(score) AS score,
                MAX(created_utc) AS latest_created_utc,
                COUNT(*) AS n_ai_sentences
            FROM sentence_level
            GROUP BY
                source,
                subreddit,
                item_type,
                metaphor_category,
                granularity,
                stance,
                post_id
        ),

        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        subreddit,
                        metaphor_category,
                        granularity,
                        stance
                    ORDER BY
                        score DESC,
                        n_ai_sentences DESC,
                        latest_created_utc DESC
                ) AS rn
            FROM item_level
        )

        SELECT
            source,
            'week',
            ?,
            ?,
            'subreddit_metaphor_granularity',
            subreddit || ' | ' || metaphor_category || ' | ' || granularity,
            subreddit,
            item_type,
            metaphor_category,
            granularity,
            stance,
            sentence_id,
            post_id,
            context_text,
            ai_sentence,
            score,
            rn
        FROM ranked
        WHERE rn <= ?;
        """,
        (SOURCE, start_ts, end_ts, week_start, week_end, top_n),
    )

    conn.commit()
    return conn.total_changes - before


def log_pipeline_run(
    conn: sqlite3.Connection,
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
        (SOURCE, None, "backfill_weekly_aggregate", n_records, status, message),
    )
    conn.commit()


def aggregate_one_week(
    conn: sqlite3.Connection,
    week_start_dt: datetime,
    week_end_dt: datetime,
    top_n: int,
    force: bool,
) -> tuple[int, int, str]:
    week_start = week_start_dt.strftime("%Y-%m-%d")
    week_end = week_end_dt.strftime("%Y-%m-%d")
    start_ts = unix_timestamp(week_start_dt)
    end_ts = unix_timestamp(week_end_dt)

    if not week_has_labeled_data(conn, start_ts, end_ts):
        return 0, 0, "empty"

    if week_already_aggregated(conn, week_start, week_end) and not force:
        return 0, 0, "skipped_existing"

    delete_existing_week(conn, week_start, week_end)

    n_metrics = insert_weekly_metrics(
        conn=conn,
        week_start=week_start,
        week_end=week_end,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    n_polarizing = insert_polarizing_examples_by_subreddit_group(
        conn=conn,
        week_start=week_start,
        week_end=week_end,
        start_ts=start_ts,
        end_ts=end_ts,
        top_n=top_n,
    )

    return n_metrics, n_polarizing, "processed"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill weekly aggregate tables across all labeled Reddit data."
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Optional start date in YYYY-MM-DD. Defaults to earliest labeled Reddit sentence week.",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Optional end date in YYYY-MM-DD. Defaults to latest labeled Reddit sentence week.",
    )

    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Top N positive/negative examples per subreddit × metaphor × granularity group.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete and recompute weeks even if aggregate rows already exist.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    conn = connect_db(DB_PATH)

    try:
        create_aggregate_tables(conn)

        data_start, data_end = get_labeled_data_bounds(conn)

        start_dt = floor_to_monday(parse_date(args.start_date)) if args.start_date else data_start
        end_dt = ceil_to_next_monday(parse_date(args.end_date)) if args.end_date else data_end

        if start_dt >= end_dt:
            raise ValueError("start date must be earlier than end date.")

        weeks = iter_weeks(start_dt, end_dt)

        logging.info(
            "Backfilling weekly aggregates | start=%s | end=%s | weeks=%s | force=%s",
            start_dt.strftime("%Y-%m-%d"),
            end_dt.strftime("%Y-%m-%d"),
            len(weeks),
            args.force,
        )

        total_metrics = 0
        total_polarizing = 0
        processed = 0
        skipped_existing = 0
        empty = 0

        for week_start_dt, week_end_dt in tqdm(
            weeks,
            desc="Backfill weekly aggregates",
            unit="week",
        ):
            n_metrics, n_polarizing, status = aggregate_one_week(
                conn=conn,
                week_start_dt=week_start_dt,
                week_end_dt=week_end_dt,
                top_n=args.top_n,
                force=args.force,
            )

            if status == "processed":
                processed += 1
                total_metrics += n_metrics
                total_polarizing += n_polarizing
            elif status == "skipped_existing":
                skipped_existing += 1
            elif status == "empty":
                empty += 1

        total_inserted = total_metrics + total_polarizing

        log_pipeline_run(
            conn=conn,
            n_records=total_inserted,
            status="success",
            message=(
                f"Backfilled weekly aggregates from {start_dt.strftime('%Y-%m-%d')} "
                f"to {end_dt.strftime('%Y-%m-%d')}. "
                f"processed_weeks={processed}; "
                f"skipped_existing={skipped_existing}; "
                f"empty_weeks={empty}; "
                f"metric_rows={total_metrics}; "
                f"polarizing_examples={total_polarizing}; "
                f"force={args.force}."
            ),
        )

    finally:
        conn.close()

    print(
        "\nDone.\n"
        f"Processed weeks: {processed}\n"
        f"Skipped existing weeks: {skipped_existing}\n"
        f"Empty weeks: {empty}\n"
        f"Inserted metric rows: {total_metrics:,}\n"
        f"Inserted polarizing examples: {total_polarizing:,}\n"
    )


if __name__ == "__main__":
    main()