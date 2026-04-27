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


# -------------------------
# Core Utilities
# -------------------------

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


# -------------------------
# Table Setup
# -------------------------

def create_shift_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS volume_shift_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT 'reddit',
            period_type TEXT NOT NULL, -- month / year
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

        CREATE INDEX IF NOT EXISTS idx_shift_period
            ON volume_shift_summary(source, period_type, period_start);

        CREATE INDEX IF NOT EXISTS idx_shift_scope
            ON volume_shift_summary(source, scope, scope_value);
        """
    )
    conn.commit()


# -------------------------
# LLM Calls
# -------------------------

def check_ollama(tags_url: str) -> None:
    try:
        r = requests.get(tags_url, timeout=10)
        r.raise_for_status()
    except Exception as exc:
        raise RuntimeError("Ollama not running") from exc


def call_ollama(prompt: str, cfg: dict[str, Any]) -> str:
    payload = {
        "model": cfg["model_name"],
        "prompt": prompt,
        "stream": False,
        "options": cfg.get("options", {}),
    }

    for attempt in range(3):
        try:
            r = requests.post(cfg["ollama_url"], json=payload, timeout=180)
            r.raise_for_status()
            return r.json().get("response", "").strip()
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(2**attempt)

    return ""


def extract_json(raw: str) -> dict[str, Any]:
    try:
        return json.loads(raw)
    except Exception:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:
                pass
    return {}


def parse_output(raw: str) -> dict[str, str]:
    parsed = extract_json(raw)

    return {
        "shift_summary": parsed.get("shift_summary", "No summary."),
        "key_transitions": parsed.get("key_transitions", ""),
        "volume_change": parsed.get("volume_change", ""),
        "stance_shift": parsed.get("stance_shift", ""),
        "metaphor_shift": parsed.get("metaphor_shift", ""),
    }


# -------------------------
# Data Fetch
# -------------------------

def get_periods(conn, period_type: str):
    if period_type == "month":
        rows = conn.execute(
            "SELECT DISTINCT month FROM monthly_llm_summary ORDER BY month"
        ).fetchall()
        return [r["month"] for r in rows]

    if period_type == "year":
        rows = conn.execute(
            "SELECT DISTINCT year FROM yearly_llm_summary ORDER BY year"
        ).fetchall()
        return [r["year"] for r in rows]


def get_scopes(conn, table: str, period_col: str, period: str):
    rows = conn.execute(
        f"""
        SELECT DISTINCT scope, scope_value
        FROM {table}
        WHERE {period_col} = ?
        """,
        (period,),
    ).fetchall()

    return [(r["scope"], r["scope_value"]) for r in rows]


def fetch_current_previous(
    conn,
    table: str,
    period_col: str,
    period: str,
    scope: str,
    scope_value: str,
):
    current = conn.execute(
        f"""
        SELECT *
        FROM {table}
        WHERE {period_col} = ?
          AND scope = ?
          AND scope_value = ?
        """,
        (period, scope, scope_value),
    ).fetchall()

    prev = conn.execute(
        f"""
        SELECT *
        FROM {table}
        WHERE {period_col} < ?
          AND scope = ?
          AND scope_value = ?
        ORDER BY {period_col} DESC
        LIMIT 1
        """,
        (period, scope, scope_value),
    ).fetchall()

    return [dict(r) for r in current], [dict(r) for r in prev]


# -------------------------
# Prompt Builder
# -------------------------

def build_input(period, scope, scope_value, current, previous):
    return f"""
PERIOD: {period}
SCOPE: {scope} | {scope_value}

CURRENT:
{json.dumps(current, indent=2)}

PREVIOUS:
{json.dumps(previous, indent=2)}

TASK:
Identify:
- volume shifts (increase/decrease)
- stance changes
- metaphor transitions
- key structural change in discourse
"""


# -------------------------
# Insert
# -------------------------

def insert_shift(
    conn,
    period_type,
    period,
    scope,
    scope_value,
    parsed,
    evidence_count,
    model,
):
    conn.execute(
        """
        INSERT OR REPLACE INTO volume_shift_summary (
            source,
            period_type,
            period_start,
            period_end,
            scope,
            scope_value,
            shift_summary,
            key_transitions,
            volume_change,
            stance_shift,
            metaphor_shift,
            evidence_count,
            comparison_period,
            model_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            SOURCE,
            period_type,
            period,
            period,
            scope,
            scope_value,
            parsed["shift_summary"],
            parsed["key_transitions"],
            parsed["volume_change"],
            parsed["stance_shift"],
            parsed["metaphor_shift"],
            evidence_count,
            "previous_period",
            model,
        ),
    )
    conn.commit()


# -------------------------
# Main Logic
# -------------------------

def run(period_type: str):
    config = load_config(CONFIG_PATH)
    llm_cfg = config["summary_llm"]

    check_ollama(llm_cfg["ollama_tags_url"])

    conn = connect_db(DB_PATH)
    create_shift_table(conn)

    table = "monthly_llm_summary" if period_type == "month" else "yearly_llm_summary"
    col = "month" if period_type == "month" else "year"

    periods = get_periods(conn, period_type)

    total = 0

    for period in tqdm(periods):
        scopes = get_scopes(conn, table, col, period)

        for scope, scope_value in scopes:
            curr, prev = fetch_current_previous(
                conn, table, col, period, scope, scope_value
            )

            if not curr or not prev:
                continue

            input_text = build_input(period, scope, scope_value, curr, prev)

            raw = call_ollama(input_text, llm_cfg)
            parsed = parse_output(raw)

            insert_shift(
                conn,
                period_type,
                period,
                scope,
                scope_value,
                parsed,
                evidence_count=len(curr),
                model=llm_cfg["model_name"],
            )

            total += 1

    print(f"\nDone. Generated {total} shift summaries.\n")


# -------------------------
# CLI
# -------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--period-type", choices=["month", "year"], default="month")
    args = parser.parse_args()

    run(args.period_type)


if __name__ == "__main__":
    main()