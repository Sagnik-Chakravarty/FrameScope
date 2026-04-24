from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import yaml


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

CONFIG_PATH = Path("config.yaml")

RAW_DIR = Path("data/raw/reddit")
PROCESSED_DIR = Path("data/processed/reddit")
CLEAN_DIR = Path("data/cleaned/reddit")
LABELED_DIR = Path("data/labeled/reddit")


def load_config(path: Path) -> dict:
    if not path.exists():
        logging.warning("No config.yaml found. Using default retention settings.")
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def clean_text(text: str) -> str:
    return " ".join(text.split()).strip()


def clean_post(post: dict) -> dict | None:
    text = clean_text((post.get("title") or "") + " " + (post.get("selftext") or ""))

    if not text:
        return None

    item_id = post.get("id")
    if not item_id:
        return None

    return {
        "item_id": item_id,
        "item_type": "post",
        "source": "reddit",
        "subreddit": post.get("subreddit"),
        "author": post.get("author"),
        "created_utc": post.get("created_utc"),
        "text": text,
        "title": post.get("title"),
        "selftext": post.get("selftext"),
        "url": post.get("url"),
        "score": post.get("score"),
        "num_comments": post.get("num_comments"),
        "link_id": None,
        "parent_id": None,
    }


def clean_comment(comment: dict) -> dict | None:
    text = clean_text(comment.get("body") or "")

    if not text:
        return None

    item_id = comment.get("id")
    if not item_id:
        return None

    return {
        "item_id": item_id,
        "item_type": "comment",
        "source": "reddit",
        "subreddit": comment.get("subreddit"),
        "author": comment.get("author"),
        "created_utc": comment.get("created_utc"),
        "text": text,
        "title": None,
        "selftext": None,
        "url": None,
        "score": comment.get("score"),
        "num_comments": None,
        "link_id": comment.get("link_id"),
        "parent_id": comment.get("parent_id"),
    }


def deduplicate_records(records: list[dict]) -> list[dict]:
    seen = set()
    deduped = []

    for record in records:
        key = (record.get("source"), record.get("item_type"), record.get("item_id"))

        if key in seen:
            continue

        seen.add(key)
        deduped.append(record)

    return deduped


def process_day(day_path: Path) -> list[dict]:
    logging.info("Processing %s", day_path)

    all_records = []

    for file in day_path.glob("*_posts*.json"):
        posts = load_json(file)

        for post in posts:
            cleaned = clean_post(post)
            if cleaned:
                all_records.append(cleaned)

    for file in day_path.glob("*_comments*.json"):
        comments = load_json(file)

        for comment in comments:
            cleaned = clean_comment(comment)
            if cleaned:
                all_records.append(cleaned)

    return deduplicate_records(all_records)


def cleanup_old_runs(base_dir: Path, retention_days: int) -> None:
    if retention_days <= 0:
        logging.info("Skipping cleanup for %s because retention_days=%s", base_dir, retention_days)
        return

    if not base_dir.exists():
        return

    cutoff_date = datetime.now() - timedelta(days=retention_days)

    for run_dir in base_dir.iterdir():
        if not run_dir.is_dir():
            continue

        try:
            run_date = datetime.strptime(run_dir.name, "%Y-%m-%d")
        except ValueError:
            logging.warning("Skipping non-date folder during cleanup: %s", run_dir)
            continue

        if run_date < cutoff_date:
            logging.info("Deleting old run folder: %s", run_dir)
            shutil.rmtree(run_dir)


def run_cleanup(config: dict) -> None:
    storage_config = config.get("storage", {})

    cleanup_old_runs(
        RAW_DIR,
        int(storage_config.get("raw_retention_days", 90)),
    )

    cleanup_old_runs(
        PROCESSED_DIR,
        int(storage_config.get("processed_retention_days", 90)),
    )

    cleanup_old_runs(
        CLEAN_DIR,
        int(storage_config.get("cleaned_retention_days", 365)),
    )

    cleanup_old_runs(
        LABELED_DIR,
        int(storage_config.get("labeled_retention_days", 365)),
    )


def main():
    config = load_config(CONFIG_PATH)

    if not RAW_DIR.exists():
        raise FileNotFoundError("No raw data found. Run fetch script first.")

    total_records = 0

    for day_path in sorted(RAW_DIR.iterdir()):
        if not day_path.is_dir():
            continue

        cleaned_data = process_day(day_path)
        output_path = CLEAN_DIR / day_path.name / "cleaned_data.json"

        save_json(cleaned_data, output_path)

        total_records += len(cleaned_data)

        logging.info(
            "Saved cleaned data for %s | records=%s",
            day_path.name,
            len(cleaned_data),
        )

    logging.info("Cleaning complete | total_records=%s", total_records)

    # Cleanup happens only after cleaning completes successfully.
    run_cleanup(config)

    logging.info("Retention cleanup complete")


if __name__ == "__main__":
    main()