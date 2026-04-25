from __future__ import annotations
import json
import logging
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
import requests
import yaml


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

SOURCE = "reddit"
CONFIG_PATH = Path("config.yaml")
DB_PATH = Path("data/database/framescope.db")


VALID_METAPHORS = {
    "Tool",
    "Assistant",
    "Genie",
    "Mirror",
    "Child",
    "Friend",
    "Animal",
    "God",
    "None",
}

VALID_GRANULARITY = {
    "General-AI",
    "Model-Specific",
    "Domain-Specific",
    "Not Applicable",
}

VALID_STANCE = {
    "Positive",
    "Neutral/Unclear",
    "Negative",
}


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def ensure_label_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
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
            PRIMARY KEY (source, sentence_id)
        );
        """
    )

    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(llm_labels);").fetchall()
    }

    if "granularity" not in existing_cols:
        conn.execute("ALTER TABLE llm_labels ADD COLUMN granularity TEXT;")

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


def fetch_unlabeled_rows(conn: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
    query = """
        SELECT
            r.sentence_id,
            r.preceding_sentence,
            r.ai_sentence,
            r.subsequent_sentence,
            r.context_text,
            r.subreddit,
            r.created_utc,
            r.score
        FROM reddit_sentence_items r
        LEFT JOIN llm_labels l
            ON r.sentence_id = l.sentence_id
            AND l.source = ?
        WHERE l.sentence_id IS NULL
        ORDER BY r.created_utc ASC
        LIMIT ?;
    """

    rows = conn.execute(query, (SOURCE, limit)).fetchall()

    return [
        {
            "sentence_id": row[0],
            "preceding_sentence": row[1],
            "ai_sentence": row[2],
            "subsequent_sentence": row[3],
            "context_text": row[4],
            "subreddit": row[5],
            "created_utc": row[6],
            "score": row[7],
        }
        for row in rows
    ]


def build_context_text(row: dict[str, Any]) -> str:
    parts = [
        row.get("preceding_sentence"),
        row.get("ai_sentence"),
        row.get("subsequent_sentence"),
    ]

    parts = [str(p).strip() for p in parts if p is not None and str(p).strip()]

    if parts:
        return " ".join(parts)

    fallback = row.get("context_text") or row.get("ai_sentence") or ""
    return str(fallback).strip()


def build_combined_prompt(
    text: str,
    metaphor_template: str,
    stance_template: str,
) -> str:
    prompt = f"""
You are labeling Reddit text related to artificial intelligence.

Use the following stance and granularity coding guide:

{stance_template}

Use the following metaphor coding guide:

{metaphor_template}

IMPORTANT FINAL OUTPUT INSTRUCTION:
Return ONLY valid JSON in this exact format:

{{
  "granularity": "General-AI | Model-Specific | Domain-Specific | Not Applicable",
  "stance": "Positive | Neutral/Unclear | Negative",
  "dominant_metaphor": "Tool | Assistant | Genie | Mirror | Child | Friend | Animal | God | None"
}}

Rules:
- Do not include explanations.
- Do not include markdown.
- Use the exact label names shown above.
- If the text is a question or informational, use "Neutral/Unclear" unless a clear stance is present.
- If no clear metaphor is explicitly present, use "None".
- Do not infer or assume metaphors.
- Only use "Model-Specific" if a specific named AI model, system, product, or company tool is mentioned.
- If the text is not meaningfully about AI, use:
  - granularity: "Not Applicable"
  - stance: "Neutral/Unclear"
  - dominant_metaphor: "None"

TEXT:
{text}
"""
    return prompt.replace("{text}", text).replace("{input_text}", text)


def call_ollama(
    prompt: str,
    ollama_url: str,
    model_name: str,
    llm_options: dict[str, Any],
    request_timeout: int,
) -> str:
    payload = {
        "model": model_name,
        "prompt": prompt,
        "stream": False,
        "options": llm_options,
    }

    response = requests.post(
        ollama_url,
        json=payload,
        timeout=request_timeout,
    )
    response.raise_for_status()

    return response.json().get("response", "").strip()


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


def clean_metaphor(value: Any) -> str:
    if not isinstance(value, str):
        return "None"

    value = value.strip().replace('"', "").replace(".", "").replace(",", "")
    value = value.split("\n")[0].strip()

    lookup = {m.lower(): m for m in VALID_METAPHORS}
    return lookup.get(value.lower(), "None")


def clean_granularity(value: Any) -> str:
    if not isinstance(value, str):
        return "Not Applicable"

    value = value.strip()

    lookup = {
        "general-ai": "General-AI",
        "general_ai": "General-AI",
        "general ai": "General-AI",
        "general": "General-AI",
        "model-specific": "Model-Specific",
        "model_specific": "Model-Specific",
        "model specific": "Model-Specific",
        "domain-specific": "Domain-Specific",
        "domain_specific": "Domain-Specific",
        "domain specific": "Domain-Specific",
        "not applicable": "Not Applicable",
        "not_applicable": "Not Applicable",
        "na": "Not Applicable",
        "n/a": "Not Applicable",
    }

    if value in VALID_GRANULARITY:
        return value

    return lookup.get(value.lower(), "Not Applicable")


def clean_stance(value: Any, granularity: str) -> str:
    if granularity == "Not Applicable":
        return "Neutral/Unclear"

    if not isinstance(value, str):
        return "Neutral/Unclear"

    value = value.strip()

    lookup = {
        "positive": "Positive",
        "neutral": "Neutral/Unclear",
        "unclear": "Neutral/Unclear",
        "neutral/unclear": "Neutral/Unclear",
        "neutral_unclear": "Neutral/Unclear",
        "neutral unclear": "Neutral/Unclear",
        "negative": "Negative",
    }

    if value in VALID_STANCE:
        return value

    return lookup.get(value.lower(), "Neutral/Unclear")


def parse_combined_output(raw_output: str) -> tuple[str, str, str, dict[str, Any]]:
    parsed = extract_json(raw_output)

    metaphor = clean_metaphor(
        parsed.get("dominant_metaphor")
        or parsed.get("metaphor_category")
        or parsed.get("metaphor")
    )

    granularity = clean_granularity(parsed.get("granularity"))
    stance = clean_stance(parsed.get("stance"), granularity)

    return metaphor, granularity, stance, parsed


def label_one_row(
    row: dict[str, Any],
    metaphor_prompt_template: str,
    stance_prompt_template: str,
    llm_config: dict[str, Any],
) -> dict[str, Any]:
    text = build_context_text(row)

    combined_prompt = build_combined_prompt(
        text=text,
        metaphor_template=metaphor_prompt_template,
        stance_template=stance_prompt_template,
    )

    try:
        start = time.time()

        raw_output = call_ollama(
            prompt=combined_prompt,
            ollama_url=llm_config["ollama_url"],
            model_name=llm_config["model_name"],
            llm_options=llm_config["options"],
            request_timeout=int(llm_config["request_timeout"]),
        )

        latency = time.time() - start

        metaphor, granularity, stance, parsed = parse_combined_output(raw_output)

        return {
            "source": SOURCE,
            "sentence_id": row["sentence_id"],
            "metaphor_category": metaphor,
            "metaphor_present": 0 if metaphor == "None" else 1,
            "granularity": granularity,
            "stance": stance,
            "confidence": None,
            "reasoning": None,
            "model_name": llm_config["model_name"],
            "latency_seconds": latency,
            "error": None,
            "raw_output": raw_output,
            "parsed_output": parsed,
        }

    except Exception as exc:
        return {
            "source": SOURCE,
            "sentence_id": row["sentence_id"],
            "metaphor_category": "None",
            "metaphor_present": 0,
            "granularity": "Not Applicable",
            "stance": "Neutral/Unclear",
            "confidence": None,
            "reasoning": None,
            "model_name": llm_config["model_name"],
            "latency_seconds": None,
            "error": str(exc),
            "raw_output": None,
            "parsed_output": None,
        }


def insert_labels(conn: sqlite3.Connection, labeled_rows: list[dict[str, Any]]) -> int:
    before = conn.total_changes

    rows_to_insert = [
        (
            r["source"],
            r["sentence_id"],
            r["metaphor_category"],
            r["metaphor_present"],
            r["granularity"],
            r["stance"],
            r["confidence"],
            r["reasoning"],
            r["model_name"],
        )
        for r in labeled_rows
        if r.get("error") is None
    ]

    conn.executemany(
        """
        INSERT OR IGNORE INTO llm_labels (
            source,
            sentence_id,
            metaphor_category,
            metaphor_present,
            granularity,
            stance,
            confidence,
            reasoning,
            model_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows_to_insert,
    )

    conn.commit()

    return conn.total_changes - before


def log_errors(labeled_rows: list[dict[str, Any]]) -> None:
    errors = [r for r in labeled_rows if r.get("error")]

    if not errors:
        return

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    path = log_dir / "05_label_llm_errors.jsonl"

    with open(path, "a", encoding="utf-8") as f:
        for row in errors:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    logging.warning("Logged %s errors to %s", len(errors), path)


def process_batch(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    metaphor_prompt_template: str,
    stance_prompt_template: str,
    llm_config: dict[str, Any],
) -> int:
    labeled_rows: list[dict[str, Any]] = []

    max_workers = int(llm_config.get("max_workers", 2))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                label_one_row,
                row,
                metaphor_prompt_template,
                stance_prompt_template,
                llm_config,
            )
            for row in rows
        ]

        for future in as_completed(futures):
            labeled_rows.append(future.result())

    inserted = insert_labels(conn, labeled_rows)
    log_errors(labeled_rows)

    return inserted


def main() -> None:
    config = load_config(CONFIG_PATH)
    llm_config = config["llm"]

    check_ollama(llm_config["ollama_tags_url"])

    metaphor_prompt_path = Path(llm_config["prompts"]["metaphor_prompt_path"])
    stance_prompt_path = Path(llm_config["prompts"]["stance_prompt_path"])

    metaphor_prompt_template = load_prompt(metaphor_prompt_path)
    stance_prompt_template = load_prompt(stance_prompt_path)

    conn = connect_db(DB_PATH)
    ensure_label_schema(conn)

    batch_size = int(llm_config.get("batch_size", 250))

    total_seen = 0
    total_inserted = 0

    while True:
        rows = fetch_unlabeled_rows(conn, batch_size)

        if not rows:
            break

        total_seen += len(rows)

        logging.info("Labeling batch | rows=%s", len(rows))

        inserted = process_batch(
            conn=conn,
            rows=rows,
            metaphor_prompt_template=metaphor_prompt_template,
            stance_prompt_template=stance_prompt_template,
            llm_config=llm_config,
        )

        total_inserted += inserted

        logging.info(
            "Batch complete | inserted=%s | total_inserted=%s",
            inserted,
            total_inserted,
        )

    conn.close()

    logging.info(
        "LLM labeling complete | rows_seen=%s | rows_inserted=%s",
        total_seen,
        total_inserted,
    )


if __name__ == "__main__":
    main()