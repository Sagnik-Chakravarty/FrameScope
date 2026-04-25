from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from arcshiftwrap.arctic_shift import (
    ArcticShiftClient,
    collect_posts_by_windows,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

RAW_DIR = Path("data/raw/reddit")

SUBREDDITS = [
    "news",
    "politics",
    "worldpolitics",
    "cryptocurrency",
    "technology",
    "askreddit",
    "moviecritic",
    "art",
    "artistlounge",
    "nostupidquestions",
    "antiwork",
]

KEYWORDS = [
    "AI",
    "Deep Fake",
    "Artificial Intelligence",
    "Chat GPT",
    "Open AI",
    "LLAMA",
    "Claude",
    "Anthropic",
    "Sora",
    "Sam Altman",
    "language model",
    "Machine Learning",
    "ChatGPT",
    "OpenAI",
    "LLM",
    "LLMs",
    "generative AI",
    "GenAI",
    "deepfake",
    "deepfakes",
    "machine learning",
    "ML",
    "large language model",
    "large language models",
    "AGI",
]

BACKFILL_START = datetime(2023, 1, 1, tzinfo=timezone.utc)
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


def save_json(data: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def keyword_filter(items: list[dict], keywords: list[str]) -> list[dict]:
    keywords_lower = [k.lower() for k in keywords]
    filtered = []

    for item in items:
        text = (item.get("title") or "") + " " + (item.get("selftext") or "")
        text = text.lower()

        if any(k in text for k in keywords_lower):
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


def main() -> None:
    client = ArcticShiftClient(
        sleep_seconds=1.0,
        max_retries=4,
        timeout=90,
    )

    logging.info("Starting POST-ONLY Reddit backfill")
    logging.info("Window: %s to %s", BACKFILL_START, BACKFILL_END)

    for window_start, window_end in month_windows(BACKFILL_START, BACKFILL_END):
        run_label = f"backfill_{window_start.strftime('%Y-%m')}"

        logging.info("Processing %s", run_label)

        for subreddit in SUBREDDITS:
            subreddit_clean = subreddit.lower().replace("r/", "")

            try:
                posts = collect_posts_by_windows(
                    client=client,
                    subreddit=subreddit_clean,
                    start=window_start,
                    end=window_end,
                    step_hours=STEP_HOURS,
                    limit=LIMIT,
                    fields=POST_FIELDS,
                )

                filtered_posts = keyword_filter(posts, KEYWORDS)

                output_path = RAW_DIR / run_label / f"{subreddit_clean}_posts.json"
                save_json(filtered_posts, output_path)

                logging.info(
                    "Saved r/%s | month=%s | total=%s | filtered=%s",
                    subreddit_clean,
                    run_label,
                    len(posts),
                    len(filtered_posts),
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