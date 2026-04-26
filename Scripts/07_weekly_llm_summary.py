from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

import requests
import yaml
from tqdm import tqdm


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

SOURCE = "reddit"
CONFIG_PATH = Path("config.yaml")
DB_PATH = Path("data/database/framescope.db")


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.row_factory = sqlite3.Row
    return conn


def create_summary_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
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

        CREATE INDEX IF NOT EXISTS idx_weekly_llm_summary_period
            ON weekly_llm_summary(source, week_start, week_end);

        CREATE INDEX IF NOT EXISTS idx_weekly_llm_summary_scope
            ON weekly_llm_summary(source, scope, scope_value);

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
    conn.commit()


def load_prompt(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing prompt file: {path}")
    return path.read_text(encoding="utf-8")


def check_ollama(tags_url: str) -> None:
    try:
        response = requests.get(tags_url, timeout=10)
        response.raise_for_status()
    except Exception as exc:
        raise RuntimeError("Ollama is not running. Start it with: ollama serve") from exc


def call_ollama(
    prompt: str,
    llm_config: dict[str, Any],
) -> str:
    payload = {
        "model": llm_config["model_name"],
        "prompt": prompt,
        "stream": False,
        "options": llm_config.get("options", {}),
    }

    max_retries = int(llm_config.get("max_retries", 2))
    retry_backoff_seconds = float(llm_config.get("retry_backoff_seconds", 1.0))
    request_timeout = int(llm_config.get("request_timeout", 180))

    attempts = max_retries + 1

    for attempt in range(attempts):
        try:
            response = requests.post(
                llm_config["ollama_url"],
                json=payload,
                timeout=request_timeout,
            )
            response.raise_for_status()
            return response.json().get("response", "").strip()
        except requests.RequestException:
            if attempt == attempts - 1:
                raise
            time.sleep(retry_backoff_seconds * (2**attempt))

    return ""


def extract_json(raw: str) -> dict[str, Any]:
    if not raw:
        return {}

    raw = raw.strip()

    try:
        return json.loads(raw)
    except Exception:
        pass

    match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            return {}

    return {}


def clean_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    value = str(value).strip()
    return value if value else fallback


def clean_dominant_stance(value: Any) -> str:
    valid = {"Positive", "Neutral/Unclear", "Negative", "Mixed"}
    value = clean_text(value, "Mixed")
    return value if value in valid else "Mixed"


def clean_dominant_granularity(value: Any) -> str:
    valid = {
        "General-AI",
        "Model-Specific",
        "Domain-Specific",
        "Mixed",
        "Not Applicable",
    }
    value = clean_text(value, "Mixed")
    return value if value in valid else "Mixed"


def parse_summary_output(raw_output: str) -> dict[str, str]:
    parsed = extract_json(raw_output)

    return {
        "summary_text": clean_text(
            parsed.get("summary_text"),
            "Summary unavailable because the model did not return a valid summary.",
        ),
        "likely_drivers": clean_text(parsed.get("likely_drivers"), ""),
        "dominant_metaphors": clean_text(parsed.get("dominant_metaphors"), "None"),
        "dominant_granularity": clean_dominant_granularity(
            parsed.get("dominant_granularity")
        ),
        "dominant_stance": clean_dominant_stance(parsed.get("dominant_stance")),
    }


def get_weeks_to_process(
    conn: sqlite3.Connection,
    week_start: str | None = None,
    week_end: str | None = None,
) -> list[tuple[str, str]]:
    if week_start and week_end:
        return [(week_start, week_end)]

    rows = conn.execute(
        """
        SELECT DISTINCT week_start, week_end
        FROM aggregate_weekly_metrics
        WHERE source = ?
        ORDER BY week_start ASC;
        """,
        (SOURCE,),
    ).fetchall()

    return [(row["week_start"], row["week_end"]) for row in rows]


def fetch_weekly_metrics(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
    where_clause: str = "",
    params: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    query = f"""
        SELECT
            subreddit,
            item_type,
            metaphor_category,
            granularity,
            stance,
            SUM(n_sentences) AS n_sentences,
            SUM(n_items) AS n_items,
            AVG(avg_score) AS avg_score
        FROM aggregate_weekly_metrics
        WHERE source = ?
          AND week_start = ?
          AND week_end = ?
          {where_clause}
        GROUP BY
            subreddit,
            item_type,
            metaphor_category,
            granularity,
            stance
        ORDER BY n_sentences DESC, n_items DESC;
    """

    rows = conn.execute(
        query,
        (SOURCE, week_start, week_end, *params),
    ).fetchall()

    return [dict(row) for row in rows]


def fetch_examples(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
    where_clause: str = "",
    params: tuple[Any, ...] = (),
    top_n: int = 10,
) -> list[dict[str, Any]]:
    query = f"""
        SELECT
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
        FROM polarizing_examples
        WHERE source = ?
          AND period_type = 'week'
          AND period_start = ?
          AND period_end = ?
          {where_clause}
        ORDER BY
            stance,
            score DESC,
            rank ASC
        LIMIT ?;
    """

    rows = conn.execute(
        query,
        (SOURCE, week_start, week_end, *params, top_n),
    ).fetchall()

    return [dict(row) for row in rows]


def summary_exists(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
    scope: str,
    scope_value: str,
    granularity: str | None,
    stance_focus: str | None,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM weekly_llm_summary
        WHERE source = ?
          AND week_start = ?
          AND week_end = ?
          AND scope = ?
          AND scope_value = ?
          AND COALESCE(granularity, '') = COALESCE(?, '')
          AND COALESCE(stance_focus, '') = COALESCE(?, '')
        LIMIT 1;
        """,
        (
            SOURCE,
            week_start,
            week_end,
            scope,
            scope_value,
            granularity,
            stance_focus,
        ),
    ).fetchone()

    return row is not None


def get_subreddits_for_week(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT subreddit
        FROM aggregate_weekly_metrics
        WHERE source = ?
          AND week_start = ?
          AND week_end = ?
          AND subreddit IS NOT NULL
        ORDER BY subreddit;
        """,
        (SOURCE, week_start, week_end),
    ).fetchall()

    return [row["subreddit"] for row in rows]


def get_granularities_for_week(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT granularity
        FROM aggregate_weekly_metrics
        WHERE source = ?
          AND week_start = ?
          AND week_end = ?
          AND granularity IS NOT NULL
          AND granularity != 'Not Applicable'
        ORDER BY granularity;
        """,
        (SOURCE, week_start, week_end),
    ).fetchall()

    return [row["granularity"] for row in rows]


def compact_metrics(metrics: list[dict[str, Any]], max_rows: int = 40) -> str:
    if not metrics:
        return "No aggregate metrics available."

    lines = []

    for row in metrics[:max_rows]:
        lines.append(
            (
                f"- subreddit={row.get('subreddit')}; "
                f"item_type={row.get('item_type')}; "
                f"metaphor={row.get('metaphor_category')}; "
                f"granularity={row.get('granularity')}; "
                f"stance={row.get('stance')}; "
                f"n_sentences={row.get('n_sentences')}; "
                f"n_items={row.get('n_items')}; "
                f"avg_score={row.get('avg_score')}"
            )
        )

    return "\n".join(lines)


def compact_examples(examples: list[dict[str, Any]], max_chars: int = 9000) -> str:
    if not examples:
        return "No polarizing examples available."

    blocks = []

    for i, ex in enumerate(examples, start=1):
        context = clean_text(ex.get("context_text"))
        if len(context) > 1200:
            context = context[:1200].rstrip() + "..."

        blocks.append(
            (
                f"Example {i}\n"
                f"subreddit: {ex.get('subreddit')}\n"
                f"metaphor: {ex.get('metaphor_category')}\n"
                f"granularity: {ex.get('granularity')}\n"
                f"stance: {ex.get('stance')}\n"
                f"score: {ex.get('score')}\n"
                f"context: {context}"
            )
        )

    output = "\n\n".join(blocks)

    if len(output) > max_chars:
        output = output[:max_chars].rstrip() + "\n\n[truncated]"

    return output


def build_input_text(
    week_start: str,
    week_end: str,
    scope: str,
    scope_value: str,
    granularity: str | None,
    stance_focus: str | None,
    metrics: list[dict[str, Any]],
    examples: list[dict[str, Any]],
) -> str:
    return f"""
PERIOD:
week_start: {week_start}
week_end: {week_end}

SCOPE:
scope: {scope}
scope_value: {scope_value}
granularity: {granularity or "All"}
stance_focus: {stance_focus or "All"}

AGGREGATE STATISTICS:
{compact_metrics(metrics)}

HIGH-SALIENCE EXAMPLES:
{compact_examples(examples)}
""".strip()


def build_prompt(prompt_template: str, input_text: str) -> str:
    if "{input_text}" in prompt_template:
        return prompt_template.replace("{input_text}", input_text)

    return f"{prompt_template}\n\nINPUT:\n{input_text}"


def insert_summary(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
    scope: str,
    scope_value: str,
    granularity: str | None,
    stance_focus: str | None,
    summary: dict[str, str],
    evidence_count: int,
    example_sentence_ids: str,
    model_name: str,
) -> int:
    before = conn.total_changes

    conn.execute(
        """
        INSERT OR IGNORE INTO weekly_llm_summary (
            source,
            week_start,
            week_end,
            scope,
            scope_value,
            granularity,
            stance_focus,
            summary_text,
            likely_drivers,
            dominant_metaphors,
            dominant_granularity,
            dominant_stance,
            evidence_count,
            example_sentence_ids,
            model_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            SOURCE,
            week_start,
            week_end,
            scope,
            scope_value,
            granularity,
            stance_focus,
            summary["summary_text"],
            summary["likely_drivers"],
            summary["dominant_metaphors"],
            summary["dominant_granularity"],
            summary["dominant_stance"],
            evidence_count,
            example_sentence_ids,
            model_name,
        ),
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
        (SOURCE, None, "weekly_llm_summary", n_records, status, message),
    )
    conn.commit()


def summarize_one_scope(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
    scope: str,
    scope_value: str,
    granularity: str | None,
    stance_focus: str | None,
    metrics_where: str,
    metrics_params: tuple[Any, ...],
    examples_where: str,
    examples_params: tuple[Any, ...],
    prompt_template: str,
    llm_config: dict[str, Any],
    top_n_examples: int,
    min_examples: int,
    force: bool,
) -> int:
    if not force and summary_exists(
        conn,
        week_start,
        week_end,
        scope,
        scope_value,
        granularity,
        stance_focus,
    ):
        return 0

    metrics = fetch_weekly_metrics(
        conn,
        week_start=week_start,
        week_end=week_end,
        where_clause=metrics_where,
        params=metrics_params,
    )

    examples = fetch_examples(
        conn,
        week_start=week_start,
        week_end=week_end,
        where_clause=examples_where,
        params=examples_params,
        top_n=top_n_examples,
    )

    if len(examples) < min_examples:
        logging.info(
            "Skipping summary | week=%s | scope=%s | value=%s | examples=%s < min=%s",
            week_start,
            scope,
            scope_value,
            len(examples),
            min_examples,
        )
        return 0

    input_text = build_input_text(
        week_start=week_start,
        week_end=week_end,
        scope=scope,
        scope_value=scope_value,
        granularity=granularity,
        stance_focus=stance_focus,
        metrics=metrics,
        examples=examples,
    )

    prompt = build_prompt(prompt_template, input_text)

    raw_output = call_ollama(prompt, llm_config)
    summary = parse_summary_output(raw_output)

    example_sentence_ids = " || ".join(
        clean_text(ex.get("sentence_id")) for ex in examples if ex.get("sentence_id")
    )

    return insert_summary(
        conn=conn,
        week_start=week_start,
        week_end=week_end,
        scope=scope,
        scope_value=scope_value,
        granularity=granularity,
        stance_focus=stance_focus,
        summary=summary,
        evidence_count=len(examples),
        example_sentence_ids=example_sentence_ids,
        model_name=llm_config["model_name"],
    )


def process_week(
    conn: sqlite3.Connection,
    week_start: str,
    week_end: str,
    prompt_template: str,
    llm_config: dict[str, Any],
    top_n_examples: int,
    min_examples: int,
    force: bool,
    include_subreddit_granularity: bool,
) -> int:
    inserted = 0

    # Overall weekly discourse.
    inserted += summarize_one_scope(
        conn=conn,
        week_start=week_start,
        week_end=week_end,
        scope="overall",
        scope_value="overall",
        granularity=None,
        stance_focus=None,
        metrics_where="",
        metrics_params=(),
        examples_where="",
        examples_params=(),
        prompt_template=prompt_template,
        llm_config=llm_config,
        top_n_examples=top_n_examples,
        min_examples=min_examples,
        force=force,
    )

    # Granularity-level summaries.
    granularities = get_granularities_for_week(conn, week_start, week_end)

    for granularity in granularities:
        inserted += summarize_one_scope(
            conn=conn,
            week_start=week_start,
            week_end=week_end,
            scope="granularity",
            scope_value=granularity,
            granularity=granularity,
            stance_focus=None,
            metrics_where="AND granularity = ?",
            metrics_params=(granularity,),
            examples_where="AND granularity = ?",
            examples_params=(granularity,),
            prompt_template=prompt_template,
            llm_config=llm_config,
            top_n_examples=top_n_examples,
            min_examples=min_examples,
            force=force,
        )

    # Subreddit-level summaries.
    subreddits = get_subreddits_for_week(conn, week_start, week_end)

    for subreddit in subreddits:
        inserted += summarize_one_scope(
            conn=conn,
            week_start=week_start,
            week_end=week_end,
            scope="subreddit",
            scope_value=subreddit,
            granularity=None,
            stance_focus=None,
            metrics_where="AND subreddit = ?",
            metrics_params=(subreddit,),
            examples_where="AND subreddit = ?",
            examples_params=(subreddit,),
            prompt_template=prompt_template,
            llm_config=llm_config,
            top_n_examples=top_n_examples,
            min_examples=min_examples,
            force=force,
        )

    # Optional: subreddit × granularity summaries.
    if include_subreddit_granularity:
        for subreddit in subreddits:
            for granularity in granularities:
                inserted += summarize_one_scope(
                    conn=conn,
                    week_start=week_start,
                    week_end=week_end,
                    scope="subreddit_granularity",
                    scope_value=f"{subreddit} | {granularity}",
                    granularity=granularity,
                    stance_focus=None,
                    metrics_where="AND subreddit = ? AND granularity = ?",
                    metrics_params=(subreddit, granularity),
                    examples_where="AND subreddit = ? AND granularity = ?",
                    examples_params=(subreddit, granularity),
                    prompt_template=prompt_template,
                    llm_config=llm_config,
                    top_n_examples=top_n_examples,
                    min_examples=min_examples,
                    force=force,
                )

    return inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate weekly LLM summaries from FrameScope aggregate tables."
    )

    parser.add_argument(
        "--week-start",
        type=str,
        default=None,
        help="Week start in YYYY-MM-DD. If omitted, processes all unsummarized weeks.",
    )

    parser.add_argument(
        "--week-end",
        type=str,
        default=None,
        help="Week end in YYYY-MM-DD. Required if --week-start is supplied.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Recreate summaries even if they already exist.",
    )

    parser.add_argument(
        "--include-subreddit-granularity",
        action="store_true",
        help="Also generate subreddit × granularity summaries. This can create many LLM calls.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.week_start and not args.week_end:
        raise ValueError("--week-end is required when --week-start is supplied.")

    config = load_config(CONFIG_PATH)
    summary_config = config["summary_llm"]

    weekly_config = summary_config.get("weekly", {})
    top_n_examples = int(weekly_config.get("top_n_examples", 10))
    min_examples = int(weekly_config.get("min_examples_for_summary", 3))

    prompt_path = Path(weekly_config["prompt_path"])
    prompt_template = load_prompt(prompt_path)

    check_ollama(summary_config["ollama_tags_url"])

    conn = connect_db(DB_PATH)

    try:
        create_summary_tables(conn)

        weeks = get_weeks_to_process(
            conn,
            week_start=args.week_start,
            week_end=args.week_end,
        )

        if not weeks:
            logging.warning("No aggregate weeks found. Run 06_weekly_aggregate.py first.")
            return

        total_inserted = 0

        with tqdm(weeks, desc="Weekly LLM summaries", unit="week") as progress:
            for week_start, week_end in progress:
                logging.info("Processing weekly summary | %s to %s", week_start, week_end)

                inserted = process_week(
                    conn=conn,
                    week_start=week_start,
                    week_end=week_end,
                    prompt_template=prompt_template,
                    llm_config=summary_config,
                    top_n_examples=top_n_examples,
                    min_examples=min_examples,
                    force=args.force,
                    include_subreddit_granularity=args.include_subreddit_granularity,
                )

                total_inserted += inserted
                progress.set_postfix({"inserted": total_inserted})

        log_pipeline_run(
            conn=conn,
            n_records=total_inserted,
            status="success",
            message=f"Generated {total_inserted} weekly LLM summary rows.",
        )

    finally:
        conn.close()

    print(f"\nDone. Inserted weekly LLM summaries: {total_inserted:,}.\n")


if __name__ == "__main__":
    main()