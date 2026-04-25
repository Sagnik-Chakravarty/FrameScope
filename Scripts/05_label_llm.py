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
from tqdm import tqdm


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

SOURCE = "reddit"
CONFIG_PATH = Path("config.yaml")
DB_PATH = Path("data/database/framescope.db")


VALID_METAPHORS = {
    "Tool", "Assistant", "Genie", "Mirror",
    "Child", "Friend", "Animal", "God", "None",
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


def count_total_sentence_rows(conn: sqlite3.Connection) -> int:
    query = """
        SELECT COUNT(*)
        FROM reddit_sentence_items
        WHERE source = ?;
    """
    return conn.execute(query, (SOURCE,)).fetchone()[0]


def count_labeled_rows(conn: sqlite3.Connection) -> int:
    query = """
        SELECT COUNT(*)
        FROM llm_labels
        WHERE source = ?;
    """
    return conn.execute(query, (SOURCE,)).fetchone()[0]


def count_unlabeled_rows(conn: sqlite3.Connection) -> int:
    query = """
        SELECT COUNT(*)
        FROM reddit_sentence_items r
        LEFT JOIN llm_labels l
            ON r.source = l.source
            AND r.sentence_id = l.sentence_id
        WHERE r.source = ?
          AND l.sentence_id IS NULL;
    """
    return conn.execute(query, (SOURCE,)).fetchone()[0]


def fetch_unlabeled_rows(conn: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
    query = """
        SELECT
            r.source,
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
            ON r.source = l.source
            AND r.sentence_id = l.sentence_id
        WHERE r.source = ?
          AND l.sentence_id IS NULL
        ORDER BY r.created_utc ASC
        LIMIT ?;
    """

    rows = conn.execute(query, (SOURCE, limit)).fetchall()

    return [
        {
            "source": row[0],
            "sentence_id": row[1],
            "preceding_sentence": row[2],
            "ai_sentence": row[3],
            "subsequent_sentence": row[4],
            "context_text": row[5],
            "subreddit": row[6],
            "created_utc": row[7],
            "score": row[8],
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


def build_prompt_parts(
    metaphor_template: str,
    stance_template: str,
) -> tuple[str, str]:
    placeholder = "__FRAMESCOPE_TEXT_PLACEHOLDER__"

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
{placeholder}
"""

    return prompt.split(placeholder, maxsplit=1)


def build_combined_prompt(text: str, prompt_prefix: str, prompt_suffix: str) -> str:
    return f"{prompt_prefix}{text}{prompt_suffix}"


def call_ollama(
    prompt: str,
    ollama_url: str,
    model_name: str,
    llm_options: dict[str, Any],
    request_timeout: int,
    max_retries: int = 1,
    retry_backoff_seconds: float = 0.5,
) -> str:
    payload = {
        "model": model_name,
        "prompt": prompt,
        "stream": False,
        "options": llm_options,
    }

    attempts = max_retries + 1

    for attempt in range(attempts):
        try:
            response = requests.post(
                ollama_url,
                json=payload,
                timeout=request_timeout,
            )
            response.raise_for_status()
            return response.json().get("response", "").strip()
        except requests.RequestException:
            if attempt == attempts - 1:
                raise
            time.sleep(retry_backoff_seconds * (2 ** attempt))

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
    prompt_prefix: str,
    prompt_suffix: str,
    llm_config: dict[str, Any],
) -> dict[str, Any]:
    text = build_context_text(row)
    combined_prompt = build_combined_prompt(text, prompt_prefix, prompt_suffix)

    try:
        start = time.time()

        raw_output = call_ollama(
            prompt=combined_prompt,
            ollama_url=llm_config["ollama_url"],
            model_name=llm_config["model_name"],
            llm_options=llm_config["options"],
            request_timeout=int(llm_config["request_timeout"]),
            max_retries=int(llm_config.get("max_retries", 1)),
            retry_backoff_seconds=float(llm_config.get("retry_backoff_seconds", 0.5)),
        )

        latency = time.time() - start
        metaphor, granularity, stance, parsed = parse_combined_output(raw_output)

        return {
            "source": row.get("source", SOURCE),
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
            "source": row.get("source", SOURCE),
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


def log_pipeline_run(
    conn: sqlite3.Connection,
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
        (
            SOURCE,
            None,
            stage,
            n_records,
            status,
            message,
        ),
    )
    conn.commit()


def process_batch(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    prompt_prefix: str,
    prompt_suffix: str,
    llm_config: dict[str, Any],
    progress_bar: tqdm | None = None,
) -> int:
    labeled_rows: list[dict[str, Any]] = []
    max_workers = int(llm_config.get("max_workers", 2))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                label_one_row,
                row,
                prompt_prefix,
                prompt_suffix,
                llm_config,
            )
            for row in rows
        ]

        for future in as_completed(futures):
            labeled_rows.append(future.result())

            if progress_bar is not None:
                progress_bar.update(1)

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

    prompt_prefix, prompt_suffix = build_prompt_parts(
        metaphor_template=metaphor_prompt_template,
        stance_template=stance_prompt_template,
    )

    conn = connect_db(DB_PATH)

    batch_size = int(llm_config.get("batch_size", 250))

    total_rows = count_total_sentence_rows(conn)
    already_labeled = count_labeled_rows(conn)
    remaining_rows = count_unlabeled_rows(conn)

    logging.info(
        "Resume status | total_sentence_rows=%s | already_labeled=%s | remaining=%s",
        total_rows,
        already_labeled,
        remaining_rows,
    )

    print(
        f"\nStarting LLM labeling from {already_labeled:,} already-labeled rows "
        f"out of {total_rows:,}. Remaining: {remaining_rows:,}.\n"
    )

    total_inserted = 0
    total_seen = 0

    with tqdm(
        total=remaining_rows,
        initial=0,
        desc="Labeling unlabeled sentences",
        unit="row",
    ) as progress_bar:

        while True:
            rows = fetch_unlabeled_rows(conn, batch_size)

            if not rows:
                break

            total_seen += len(rows)

            logging.info("Labeling batch | rows=%s", len(rows))

            inserted = process_batch(
                conn=conn,
                rows=rows,
                prompt_prefix=prompt_prefix,
                prompt_suffix=prompt_suffix,
                llm_config=llm_config,
                progress_bar=progress_bar,
            )

            total_inserted += inserted

            logging.info(
                "Batch complete | inserted=%s | total_inserted_this_run=%s",
                inserted,
                total_inserted,
            )

    log_pipeline_run(
        conn=conn,
        stage="label_llm",
        n_records=total_inserted,
        status="success",
        message=(
            f"Labeled {total_inserted} new sentence records using "
            f"{llm_config['model_name']}. Previously labeled: {already_labeled}. "
            f"Remaining at start: {remaining_rows}."
        ),
    )

    final_labeled = count_labeled_rows(conn)
    final_remaining = count_unlabeled_rows(conn)

    conn.close()

    logging.info(
        "LLM labeling complete | rows_seen_this_run=%s | rows_inserted_this_run=%s | total_labeled_now=%s | remaining_now=%s",
        total_seen,
        total_inserted,
        final_labeled,
        final_remaining,
    )

    print(
        f"\nDone. Inserted this run: {total_inserted:,}. "
        f"Total labeled now: {final_labeled:,}. Remaining: {final_remaining:,}.\n"
    )


if __name__ == "__main__":
    main()