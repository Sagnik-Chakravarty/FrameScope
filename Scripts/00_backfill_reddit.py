from __future__ import annotations

import json
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from arcshiftwrap.arctic_shift import (
    ArcticShiftClient,
    collect_posts_by_windows,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

CONFIG_PATH = Path("config.yaml")
RAW_DIR = Path("data/raw/reddit")

BACKFILL_START = datetime(2020, 1, 1, tzinfo=timezone.utc)
BACKFILL_END = datetime(2026, 4, 21, tzinfo=timezone.utc)

STEP_HOURS = 24
LIMIT = 100

POST_FIELDS = [
    "id",
    "subreddit",
    "author",
    "created_utc",
    "title",
    "selftext",
    "url",
    "score",
    "num_comments",
]


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_json(data: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def keyword_filter(items: list[dict], keywords: list[str]) -> list[dict]:
    if not keywords:
        return items

    keywords_lower = [k.lower() for k in keywords if str(k).strip()]
    filtered = []

    for item in items:
        text = (item.get("title") or "") + " " + (item.get("selftext") or "")
        text = text.lower()

        if any(keyword in text for keyword in keywords_lower):
            filtered.append(item)

    return filtered


def month_windows(start: datetime, end: datetime):
    current = datetime(start.year, start.month, 1, tzinfo=timezone.utc)

    while current < end:
        if current.month == 12:
            next_month = datetime(current.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            next_month = datetime(current.year, current.month + 1, 1, tzinfo=timezone.utc)

        window_start = max(current, start)
        window_end = min(next_month, end)

        yield window_start, window_end

        current = next_month


def backfill_subreddit_month(
    subreddit: str,
    run_label: str,
    window_start: datetime,
    window_end: datetime,
    keywords: list[str],
    step_hours: int,
    limit: int,
    api_sleep_seconds: float,
    api_max_retries: int,
    api_timeout: int,
) -> tuple[str, int, int]:
    subreddit_clean = str(subreddit).lower().replace("r/", "")

    client = ArcticShiftClient(
        sleep_seconds=api_sleep_seconds,
        max_retries=api_max_retries,
        timeout=api_timeout,
    )

    posts = collect_posts_by_windows(
        client=client,
        subreddit=subreddit_clean,
        start=window_start,
        end=window_end,
        step_hours=step_hours,
        limit=limit,
        fields=POST_FIELDS,
    )

    filtered_posts = keyword_filter(posts, keywords)

    output_path = RAW_DIR / run_label / f"{subreddit_clean}_posts.json"
    save_json(filtered_posts, output_path)

    return subreddit_clean, len(posts), len(filtered_posts)


def main() -> None:
    config = load_config(CONFIG_PATH)

    reddit_config = config.get("reddit", {})
    sentence_config = config.get("sentence_preprocess", {})

    subreddits = reddit_config.get("subreddits", [])
    keywords = reddit_config.get("keywords") or sentence_config.get("ai_keywords", [])

    if not subreddits:
        raise ValueError("No subreddits found in config.yaml under reddit.subreddits")

    if not keywords:
        raise ValueError(
            "No keywords found in config.yaml under reddit.keywords or sentence_preprocess.ai_keywords"
        )

    step_hours = int(reddit_config.get("step_hours", STEP_HOURS))
    limit = int(reddit_config.get("limit", LIMIT))
    api_sleep_seconds = float(config.get("api", {}).get("sleep_seconds", 1.0))
    api_max_retries = int(config.get("api", {}).get("max_retries", 4))
    api_timeout = int(config.get("api", {}).get("timeout", 90))

    requested_workers = int(reddit_config.get("backfill_processes", len(subreddits)))
    max_workers = max(1, min(requested_workers, len(subreddits)))

    logging.info("Starting POST-ONLY Reddit backfill")
    logging.info("Window: %s to %s", BACKFILL_START, BACKFILL_END)
    logging.info("Subreddits loaded from config: %s", len(subreddits))
    logging.info("Keywords loaded from config: %s", len(keywords))
    logging.info("Backfill processes: %s", max_workers)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for window_start, window_end in month_windows(BACKFILL_START, BACKFILL_END):
            run_label = f"backfill_{window_start.strftime('%Y-%m')}"

            logging.info("Processing %s", run_label)

            future_to_subreddit = {
                executor.submit(
                    backfill_subreddit_month,
                    subreddit,
                    run_label,
                    window_start,
                    window_end,
                    keywords,
                    step_hours,
                    limit,
                    api_sleep_seconds,
                    api_max_retries,
                    api_timeout,
                ): subreddit
                for subreddit in subreddits
            }

            for future in as_completed(future_to_subreddit):
                subreddit = future_to_subreddit[future]
                subreddit_clean = str(subreddit).lower().replace("r/", "")

                try:
                    subreddit_name, total_posts, filtered_posts = future.result()
                    logging.info(
                        "Saved r/%s | month=%s | total=%s | filtered=%s",
                        subreddit_name,
                        run_label,
                        total_posts,
                        filtered_posts,
                    )
                except Exception:
                    logging.exception(
                        "Failed r/%s in %s. Skipping.",
                        subreddit_clean,
                        run_label,
                    )
                    continue

    logging.info("Backfill complete (posts only)")


if __name__ == "__main__":
    main()