from __future__ import annotations

import json
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from arcshiftwrap.arctic_shift import (
    ArcticShiftClient,
    collect_comments_by_windows,
    collect_posts_by_windows,
    utc_now,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

CONFIG_PATH = Path("config.yaml")

RAW_DIR = Path("data/raw/reddit")
FILTERED_DIR = Path("data/processed/reddit")

POST_FIELDS = [
    "id",
    "subreddit",
    "author",
    "author_fullname",
    "author_flair_text",
    "created_utc",
    "retrieved_on",
    "title",
    "selftext",
    "url",
    "score",
    "num_comments",
    "link_flair_text",
    "over_18",
    "spoiler",
    "post_hint",
]

COMMENT_FIELDS = [
    "id",
    "subreddit",
    "author",
    "author_fullname",
    "author_flair_text",
    "created_utc",
    "retrieved_on",
    "body",
    "score",
    "link_id",
    "parent_id",
]


def load_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_json(data: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def keyword_filter(
    items: list[dict],
    keywords: list[str],
    text_fields: list[str],
) -> list[dict]:
    if not keywords:
        return items

    keywords_lower = [keyword.lower() for keyword in keywords]

    filtered = []

    for item in items:
        text_parts = []

        for field in text_fields:
            value = item.get(field)
            if value:
                text_parts.append(str(value).lower())

        combined_text = " ".join(text_parts)

        if any(keyword in combined_text for keyword in keywords_lower):
            filtered.append(item)

    return filtered


def fetch_subreddit(
    subreddit: str,
    run_date: str,
    keywords: list[str],
    start_time,
    end_time,
    step_hours: int,
    limit: int,
    api_sleep_seconds: float,
    api_max_retries: int,
    api_timeout: int,
) -> tuple[str, int, int, int, int]:
    subreddit_clean = subreddit.lower().replace("r/", "")

    client = ArcticShiftClient(
        sleep_seconds=api_sleep_seconds,
        max_retries=api_max_retries,
        timeout=api_timeout,
    )

    posts = collect_posts_by_windows(
        client=client,
        subreddit=subreddit_clean,
        start=start_time,
        end=end_time,
        step_hours=step_hours,
        limit=limit,
        fields=POST_FIELDS,
    )

    comments = collect_comments_by_windows(
        client=client,
        subreddit=subreddit_clean,
        start=start_time,
        end=end_time,
        step_hours=step_hours,
        limit=limit,
        fields=COMMENT_FIELDS,
    )

    filtered_posts = keyword_filter(
        posts,
        keywords=keywords,
        text_fields=["title", "selftext"],
    )

    filtered_comments = keyword_filter(
        comments,
        keywords=keywords,
        text_fields=["body"],
    )

    raw_post_path = RAW_DIR / run_date / f"{subreddit_clean}_posts.json"
    raw_comment_path = RAW_DIR / run_date / f"{subreddit_clean}_comments.json"

    filtered_post_path = FILTERED_DIR / run_date / f"{subreddit_clean}_posts_filtered.json"
    filtered_comment_path = FILTERED_DIR / run_date / f"{subreddit_clean}_comments_filtered.json"

    save_json(posts, raw_post_path)
    save_json(comments, raw_comment_path)
    save_json(filtered_posts, filtered_post_path)
    save_json(filtered_comments, filtered_comment_path)

    return (
        subreddit_clean,
        len(posts),
        len(filtered_posts),
        len(comments),
        len(filtered_comments),
    )


def main() -> None:
    config = load_config(CONFIG_PATH)

    reddit_config = config["reddit"]

    subreddits = reddit_config["subreddits"]
    keywords = reddit_config.get("keywords", [])
    requested_workers = int(reddit_config.get("fetch_processes", len(subreddits)))
    max_workers = max(1, min(requested_workers, len(subreddits)))
    api_sleep_seconds = float(config.get("api", {}).get("sleep_seconds", 1.0))
    api_max_retries = int(config.get("api", {}).get("max_retries", 4))
    api_timeout = int(config.get("api", {}).get("timeout", 90))

    lookback_days = int(reddit_config.get("lookback_days", 7))
    freshness_lag_hours = int(reddit_config.get("freshness_lag_hours", 48))
    step_hours = int(reddit_config.get("step_hours", 24))
    limit = int(reddit_config.get("limit", 100))

    end_time = utc_now() - timedelta(hours=freshness_lag_hours)
    start_time = end_time - timedelta(days=lookback_days)

    run_date = utc_now().strftime("%Y-%m-%d")

    logging.info("Starting Reddit fetch")
    logging.info("Window: %s to %s", start_time, end_time)
    logging.info("Subreddits: %s", ", ".join(subreddits))
    logging.info("Keywords: %s", ", ".join(keywords))
    logging.info("Fetch processes: %s", max_workers)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_subreddit = {
            executor.submit(
                fetch_subreddit,
                subreddit,
                run_date,
                keywords,
                start_time,
                end_time,
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
            subreddit_clean = subreddit.lower().replace("r/", "")

            try:
                _, raw_posts, filtered_posts, raw_comments, filtered_comments = future.result()
                logging.info(
                    "Saved r/%s | raw_posts=%s | filtered_posts=%s | raw_comments=%s | filtered_comments=%s",
                    subreddit_clean,
                    raw_posts,
                    filtered_posts,
                    raw_comments,
                    filtered_comments,
                )
            except Exception:
                logging.exception("Failed r/%s. Skipping.", subreddit_clean)
                continue

    logging.info("Fetch complete")


if __name__ == "__main__":
    main()