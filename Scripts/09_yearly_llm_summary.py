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


def create_yearly_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
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

        CREATE INDEX IF NOT EXISTS idx_yearly_llm_summary_period
            ON yearly_llm_summary(source, year);

        CREATE INDEX IF NOT EXISTS idx_yearly_llm_summary_scope
            ON yearly_llm_summary(source, scope, scope_value);

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
            "Yearly summary unavailable because the model did not return a valid summary.",
        ),
        "likely_drivers": clean_text(parsed.get("likely_drivers"), ""),
        "dominant_metaphors": clean_text(parsed.get("dominant_metaphors"), "None"),
        "dominant_granularity": clean_dominant_granularity(
            parsed.get("dominant_granularity")
        ),
        "dominant_stance": clean_dominant_stance(parsed.get("dominant_stance")),
    }


def get_years_to_process(conn: sqlite3.Connection, year: str | None = None) -> list[str]:
    if year:
        return [year]

    rows = conn.execute(
        """
        SELECT DISTINCT substr(month, 1, 4) AS year
        FROM monthly_llm_summary
        WHERE source = ?
        ORDER BY year ASC;
        """,
        (SOURCE,),
    ).fetchall()

    return [row["year"] for row in rows]


def get_scopes_for_year(conn: sqlite3.Connection, year: str) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT DISTINCT scope, scope_value
        FROM monthly_llm_summary
        WHERE source = ?
          AND substr(month, 1, 4) = ?
        ORDER BY scope, scope_value;
        """,
        (SOURCE, year),
    ).fetchall()

    return [(row["scope"], row["scope_value"]) for row in rows]


def yearly_summary_exists(
    conn: sqlite3.Connection,
    year: str,
    scope: str,
    scope_value: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM yearly_llm_summary
        WHERE source = ?
          AND year = ?
          AND scope = ?
          AND scope_value = ?
        LIMIT 1;
        """,
        (SOURCE, year, scope, scope_value),
    ).fetchone()

    return row is not None


def delete_existing_year_scope(
    conn: sqlite3.Connection,
    year: str,
    scope: str,
    scope_value: str,
) -> None:
    conn.execute(
        """
        DELETE FROM yearly_llm_summary
        WHERE source = ?
          AND year = ?
          AND scope = ?
          AND scope_value = ?;
        """,
        (SOURCE, year, scope, scope_value),
    )
    conn.commit()


def fetch_monthly_summaries_for_year_scope(
    conn: sqlite3.Connection,
    year: str,
    scope: str,
    scope_value: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            id,
            month,
            scope,
            scope_value,
            summary_text,
            likely_drivers,
            dominant_metaphors,
            dominant_granularity,
            dominant_stance,
            weeks_covered
        FROM monthly_llm_summary
        WHERE source = ?
          AND substr(month, 1, 4) = ?
          AND scope = ?
          AND scope_value = ?
        ORDER BY month ASC;
        """,
        (SOURCE, year, scope, scope_value),
    ).fetchall()

    return [dict(row) for row in rows]


def compact_monthly_summaries(rows: list[dict[str, Any]], max_chars: int = 12000) -> str:
    if not rows:
        return "No monthly summaries available."

    blocks = []

    for row in rows:
        blocks.append(
            (
                f"Month: {row.get('month')}\n"
                f"scope: {row.get('scope')}\n"
                f"scope_value: {row.get('scope_value')}\n"
                f"dominant_metaphors: {row.get('dominant_metaphors')}\n"
                f"dominant_granularity: {row.get('dominant_granularity')}\n"
                f"dominant_stance: {row.get('dominant_stance')}\n"
                f"weeks_covered: {row.get('weeks_covered')}\n"
                f"summary_text: {row.get('summary_text')}\n"
                f"likely_drivers: {row.get('likely_drivers')}"
            )
        )

    output = "\n\n---\n\n".join(blocks)

    if len(output) > max_chars:
        output = output[:max_chars].rstrip() + "\n\n[truncated]"

    return output


def build_input_text(
    year: str,
    scope: str,
    scope_value: str,
    monthly_rows: list[dict[str, Any]],
) -> str:
    return f"""
YEAR:
{year}

SCOPE:
scope: {scope}
scope_value: {scope_value}

MONTHLY SUMMARIES:
{compact_monthly_summaries(monthly_rows)}
""".strip()


def build_prompt(prompt_template: str, input_text: str) -> str:
    if "{input_text}" in prompt_template:
        return prompt_template.replace("{input_text}", input_text)
    return f"{prompt_template}\n\nINPUT:\n{input_text}"


def insert_yearly_summary(
    conn: sqlite3.Connection,
    year: str,
    scope: str,
    scope_value: str,
    summary: dict[str, str],
    months_covered: int,
    monthly_summary_ids: str,
    model_name: str,
) -> int:
    before = conn.total_changes

    conn.execute(
        """
        INSERT OR IGNORE INTO yearly_llm_summary (
            source,
            year,
            scope,
            scope_value,
            summary_text,
            likely_drivers,
            dominant_metaphors,
            dominant_granularity,
            dominant_stance,
            months_covered,
            monthly_summary_ids,
            model_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            SOURCE,
            year,
            scope,
            scope_value,
            summary["summary_text"],
            summary["likely_drivers"],
            summary["dominant_metaphors"],
            summary["dominant_granularity"],
            summary["dominant_stance"],
            months_covered,
            monthly_summary_ids,
            model_name,
        ),
    )

    conn.commit()
    return conn.total_changes - before


def summarize_year_scope(
    conn: sqlite3.Connection,
    year: str,
    scope: str,
    scope_value: str,
    prompt_template: str,
    llm_config: dict[str, Any],
    min_monthly_summaries: int,
    force: bool,
) -> int:
    if yearly_summary_exists(conn, year, scope, scope_value):
        if not force:
            return 0
        delete_existing_year_scope(conn, year, scope, scope_value)

    monthly_rows = fetch_monthly_summaries_for_year_scope(
        conn=conn,
        year=year,
        scope=scope,
        scope_value=scope_value,
    )

    if len(monthly_rows) < min_monthly_summaries:
        logging.info(
            "Skipping yearly summary | year=%s | scope=%s | value=%s | monthly_summaries=%s < min=%s",
            year,
            scope,
            scope_value,
            len(monthly_rows),
            min_monthly_summaries,
        )
        return 0

    input_text = build_input_text(
        year=year,
        scope=scope,
        scope_value=scope_value,
        monthly_rows=monthly_rows,
    )

    prompt = build_prompt(prompt_template, input_text)
    raw_output = call_ollama(prompt, llm_config)
    summary = parse_summary_output(raw_output)

    monthly_summary_ids = " || ".join(str(row["id"]) for row in monthly_rows)

    return insert_yearly_summary(
        conn=conn,
        year=year,
        scope=scope,
        scope_value=scope_value,
        summary=summary,
        months_covered=len(monthly_rows),
        monthly_summary_ids=monthly_summary_ids,
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
        (SOURCE, None, "yearly_llm_summary", n_records, status, message),
    )
    conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate yearly LLM summaries from monthly FrameScope summaries."
    )

    parser.add_argument(
        "--year",
        type=str,
        default=None,
        help="Year in YYYY format. If omitted, processes all years with monthly summaries.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete and recreate yearly summaries if they already exist.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_config(CONFIG_PATH)
    summary_config = config["summary_llm"]

    yearly_config = summary_config.get("yearly", {})
    min_monthly_summaries = int(yearly_config.get("min_monthly_summaries", 2))
    prompt_path = Path(yearly_config["prompt_path"])
    prompt_template = load_prompt(prompt_path)

    check_ollama(summary_config["ollama_tags_url"])

    conn = connect_db(DB_PATH)

    try:
        create_yearly_table(conn)

        years = get_years_to_process(conn, year=args.year)

        if not years:
            logging.warning("No monthly summaries found. Run 08_monthly_llm_summary.py first.")
            return

        total_inserted = 0

        with tqdm(years, desc="Yearly LLM summaries", unit="year") as progress:
            for year in progress:
                scopes = get_scopes_for_year(conn, year)

                for scope, scope_value in scopes:
                    inserted = summarize_year_scope(
                        conn=conn,
                        year=year,
                        scope=scope,
                        scope_value=scope_value,
                        prompt_template=prompt_template,
                        llm_config=summary_config,
                        min_monthly_summaries=min_monthly_summaries,
                        force=args.force,
                    )

                    total_inserted += inserted

                progress.set_postfix({"inserted": total_inserted})

        log_pipeline_run(
            conn=conn,
            n_records=total_inserted,
            status="success",
            message=f"Generated {total_inserted} yearly LLM summary rows.",
        )

    finally:
        conn.close()

    print(f"\nDone. Inserted yearly LLM summaries: {total_inserted:,}.\n")


if __name__ == "__main__":
    main()