from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path

import yaml

from Framescope.arctic_shift import (
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


def main() -> None:
    config = load_config(CONFIG_PATH)

    reddit_config = config["reddit"]

    subreddits = reddit_config["subreddits"]
    keywords = reddit_config.get("keywords", [])

    lookback_days = int(reddit_config.get("lookback_days", 7))
    freshness_lag_hours = int(reddit_config.get("freshness_lag_hours", 48))
    step_hours = int(reddit_config.get("step_hours", 24))
    limit = int(reddit_config.get("limit", 100))

    end_time = utc_now() - timedelta(hours=freshness_lag_hours)
    start_time = end_time - timedelta(days=lookback_days)

    run_date = utc_now().strftime("%Y-%m-%d")

    client = ArcticShiftClient(
        sleep_seconds=float(config.get("api", {}).get("sleep_seconds", 1.0)),
        max_retries=int(config.get("api", {}).get("max_retries", 4)),
        timeout=int(config.get("api", {}).get("timeout", 90)),
    )

    logging.info("Starting Reddit fetch")
    logging.info("Window: %s to %s", start_time, end_time)
    logging.info("Subreddits: %s", ", ".join(subreddits))
    logging.info("Keywords: %s", ", ".join(keywords))

    for subreddit in subreddits:
        subreddit_clean = subreddit.lower().replace("r/", "")

        logging.info("Fetching r/%s", subreddit_clean)

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

        logging.info(
            "Saved r/%s | raw_posts=%s | filtered_posts=%s | raw_comments=%s | filtered_comments=%s",
            subreddit_clean,
            len(posts),
            len(filtered_posts),
            len(comments),
            len(filtered_comments),
        )

    logging.info("Fetch complete")


if __name__ == "__main__":
    main()