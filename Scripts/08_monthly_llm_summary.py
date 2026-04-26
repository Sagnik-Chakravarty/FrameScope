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


def create_monthly_table(conn: sqlite3.Connection) -> None:
    monthly_table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'monthly_llm_summary'
        LIMIT 1;
        """
    ).fetchone()

    if monthly_table_exists:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(monthly_llm_summary);").fetchall()
        }

        # Migrate legacy schema that lacked scope/scope_value fields.
        if "scope" not in columns or "scope_value" not in columns:
            logging.warning(
                "Detected legacy monthly_llm_summary schema. Migrating table to include scope columns."
            )

            conn.executescript(
                """
                ALTER TABLE monthly_llm_summary RENAME TO monthly_llm_summary_legacy;

                CREATE TABLE monthly_llm_summary (
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

                INSERT INTO monthly_llm_summary (
                    source,
                    month,
                    scope,
                    scope_value,
                    summary_text,
                    likely_drivers,
                    dominant_metaphors,
                    dominant_granularity,
                    dominant_stance,
                    weeks_covered,
                    weekly_summary_ids,
                    model_name,
                    generated_at
                )
                SELECT
                    COALESCE(source, 'reddit') AS source,
                    COALESCE(month, 'unknown') AS month,
                    'global' AS scope,
                    'all' AS scope_value,
                    COALESCE(summary_text, '') AS summary_text,
                    '' AS likely_drivers,
                    dominant_metaphors,
                    dominant_granularity,
                    dominant_stance,
                    NULL AS weeks_covered,
                    NULL AS weekly_summary_ids,
                    model_name,
                    generated_at
                FROM monthly_llm_summary_legacy;
                """
            )

    conn.executescript(
        """
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

        CREATE INDEX IF NOT EXISTS idx_monthly_llm_summary_period
            ON monthly_llm_summary(source, month);

        CREATE INDEX IF NOT EXISTS idx_monthly_llm_summary_scope
            ON monthly_llm_summary(source, scope, scope_value);

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


def call_ollama(prompt: str, llm_config: dict[str, Any]) -> str:
    payload = {
        "model": llm_config["model_name"],
        "prompt": prompt,
        "stream": False,
        "options": llm_config.get("options", {}),
    }

    max_retries = int(llm_config.get("max_retries", 2))
    retry_backoff_seconds = float(llm_config.get("retry_backoff_seconds", 1.0))
    request_timeout = int(llm_config.get("request_timeout", 180))

    for attempt in range(max_retries + 1):
        try:
            response = requests.post(
                llm_config["ollama_url"],
                json=payload,
                timeout=request_timeout,
            )
            response.raise_for_status()
            return response.json().get("response", "").strip()
        except requests.RequestException:
            if attempt == max_retries:
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
            "Monthly summary unavailable because the model did not return a valid summary.",
        ),
        "likely_drivers": clean_text(parsed.get("likely_drivers"), ""),
        "dominant_metaphors": clean_text(parsed.get("dominant_metaphors"), "None"),
        "dominant_granularity": clean_dominant_granularity(
            parsed.get("dominant_granularity")
        ),
        "dominant_stance": clean_dominant_stance(parsed.get("dominant_stance")),
    }


def get_months_to_process(
    conn: sqlite3.Connection,
    month: str | None = None,
) -> list[str]:
    if month:
        return [month]

    rows = conn.execute(
        """
        SELECT DISTINCT substr(week_start, 1, 7) AS month
        FROM weekly_llm_summary
        WHERE source = ?
        ORDER BY month ASC;
        """,
        (SOURCE,),
    ).fetchall()

    return [row["month"] for row in rows]


def get_scopes_for_month(conn: sqlite3.Connection, month: str) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT DISTINCT scope, scope_value
        FROM weekly_llm_summary
        WHERE source = ?
          AND substr(week_start, 1, 7) = ?
        ORDER BY scope, scope_value;
        """,
        (SOURCE, month),
    ).fetchall()

    return [(row["scope"], row["scope_value"]) for row in rows]


def monthly_summary_exists(
    conn: sqlite3.Connection,
    month: str,
    scope: str,
    scope_value: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM monthly_llm_summary
        WHERE source = ?
          AND month = ?
          AND scope = ?
          AND scope_value = ?
        LIMIT 1;
        """,
        (SOURCE, month, scope, scope_value),
    ).fetchone()

    return row is not None


def delete_existing_month_scope(
    conn: sqlite3.Connection,
    month: str,
    scope: str,
    scope_value: str,
) -> None:
    conn.execute(
        """
        DELETE FROM monthly_llm_summary
        WHERE source = ?
          AND month = ?
          AND scope = ?
          AND scope_value = ?;
        """,
        (SOURCE, month, scope, scope_value),
    )
    conn.commit()


def fetch_weekly_summaries_for_month_scope(
    conn: sqlite3.Connection,
    month: str,
    scope: str,
    scope_value: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            id,
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
            evidence_count
        FROM weekly_llm_summary
        WHERE source = ?
          AND substr(week_start, 1, 7) = ?
          AND scope = ?
          AND scope_value = ?
        ORDER BY week_start ASC;
        """,
        (SOURCE, month, scope, scope_value),
    ).fetchall()

    return [dict(row) for row in rows]


def compact_weekly_summaries(rows: list[dict[str, Any]], max_chars: int = 10000) -> str:
    if not rows:
        return "No weekly summaries available."

    blocks = []

    for row in rows:
        blocks.append(
            (
                f"Week: {row.get('week_start')} to {row.get('week_end')}\n"
                f"scope: {row.get('scope')}\n"
                f"scope_value: {row.get('scope_value')}\n"
                f"dominant_metaphors: {row.get('dominant_metaphors')}\n"
                f"dominant_granularity: {row.get('dominant_granularity')}\n"
                f"dominant_stance: {row.get('dominant_stance')}\n"
                f"evidence_count: {row.get('evidence_count')}\n"
                f"summary_text: {row.get('summary_text')}\n"
                f"likely_drivers: {row.get('likely_drivers')}"
            )
        )

    output = "\n\n---\n\n".join(blocks)

    if len(output) > max_chars:
        output = output[:max_chars].rstrip() + "\n\n[truncated]"

    return output


def build_input_text(
    month: str,
    scope: str,
    scope_value: str,
    weekly_rows: list[dict[str, Any]],
) -> str:
    return f"""
MONTH:
{month}

SCOPE:
scope: {scope}
scope_value: {scope_value}

WEEKLY SUMMARIES:
{compact_weekly_summaries(weekly_rows)}
""".strip()


def build_prompt(prompt_template: str, input_text: str) -> str:
    if "{input_text}" in prompt_template:
        return prompt_template.replace("{input_text}", input_text)
    return f"{prompt_template}\n\nINPUT:\n{input_text}"


def insert_monthly_summary(
    conn: sqlite3.Connection,
    month: str,
    scope: str,
    scope_value: str,
    summary: dict[str, str],
    weeks_covered: int,
    weekly_summary_ids: str,
    model_name: str,
) -> int:
    before = conn.total_changes

    conn.execute(
        """
        INSERT OR IGNORE INTO monthly_llm_summary (
            source,
            month,
            scope,
            scope_value,
            summary_text,
            likely_drivers,
            dominant_metaphors,
            dominant_granularity,
            dominant_stance,
            weeks_covered,
            weekly_summary_ids,
            model_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            SOURCE,
            month,
            scope,
            scope_value,
            summary["summary_text"],
            summary["likely_drivers"],
            summary["dominant_metaphors"],
            summary["dominant_granularity"],
            summary["dominant_stance"],
            weeks_covered,
            weekly_summary_ids,
            model_name,
        ),
    )

    conn.commit()
    return conn.total_changes - before


def summarize_month_scope(
    conn: sqlite3.Connection,
    month: str,
    scope: str,
    scope_value: str,
    prompt_template: str,
    llm_config: dict[str, Any],
    min_weekly_summaries: int,
    force: bool,
) -> int:
    if monthly_summary_exists(conn, month, scope, scope_value):
        if not force:
            return 0
        delete_existing_month_scope(conn, month, scope, scope_value)

    weekly_rows = fetch_weekly_summaries_for_month_scope(
        conn=conn,
        month=month,
        scope=scope,
        scope_value=scope_value,
    )

    if len(weekly_rows) < min_weekly_summaries:
        logging.info(
            "Skipping monthly summary | month=%s | scope=%s | value=%s | weekly_summaries=%s < min=%s",
            month,
            scope,
            scope_value,
            len(weekly_rows),
            min_weekly_summaries,
        )
        return 0

    input_text = build_input_text(
        month=month,
        scope=scope,
        scope_value=scope_value,
        weekly_rows=weekly_rows,
    )

    prompt = build_prompt(prompt_template, input_text)
    raw_output = call_ollama(prompt, llm_config)
    summary = parse_summary_output(raw_output)

    weekly_summary_ids = " || ".join(str(row["id"]) for row in weekly_rows)

    return insert_monthly_summary(
        conn=conn,
        month=month,
        scope=scope,
        scope_value=scope_value,
        summary=summary,
        weeks_covered=len(weekly_rows),
        weekly_summary_ids=weekly_summary_ids,
        model_name=llm_config["model_name"],
    )


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
        (SOURCE, None, "monthly_llm_summary", n_records, status, message),
    )
    conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate monthly LLM summaries from weekly FrameScope summaries."
    )

    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="Month in YYYY-MM format. If omitted, processes all months with weekly summaries.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete and recreate monthly summaries if they already exist.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_config(CONFIG_PATH)
    summary_config = config["summary_llm"]

    monthly_config = summary_config.get("monthly", {})
    min_weekly_summaries = int(monthly_config.get("min_weekly_summaries", 2))
    prompt_path = Path(monthly_config["prompt_path"])
    prompt_template = load_prompt(prompt_path)

    check_ollama(summary_config["ollama_tags_url"])

    conn = connect_db(DB_PATH)

    try:
        create_monthly_table(conn)

        months = get_months_to_process(conn, month=args.month)

        if not months:
            logging.warning("No weekly summaries found. Run 07_weekly_llm_summary.py first.")
            return

        total_inserted = 0

        with tqdm(months, desc="Monthly LLM summaries", unit="month") as progress:
            for month in progress:
                scopes = get_scopes_for_month(conn, month)

                for scope, scope_value in scopes:
                    inserted = summarize_month_scope(
                        conn=conn,
                        month=month,
                        scope=scope,
                        scope_value=scope_value,
                        prompt_template=prompt_template,
                        llm_config=summary_config,
                        min_weekly_summaries=min_weekly_summaries,
                        force=args.force,
                    )

                    total_inserted += inserted

                progress.set_postfix({"inserted": total_inserted})

        log_pipeline_run(
            conn=conn,
            n_records=total_inserted,
            status="success",
            message=f"Generated {total_inserted} monthly LLM summary rows.",
        )

    finally:
        conn.close()

    print(f"\nDone. Inserted monthly LLM summaries: {total_inserted:,}.\n")


if __name__ == "__main__":
    main()