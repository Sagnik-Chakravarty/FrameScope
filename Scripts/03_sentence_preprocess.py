from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import spacy
import yaml


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

CONFIG_PATH = Path("config.yaml")

CLEAN_DIR = Path("data/cleaned/reddit")
SENTENCE_DIR = Path("data/sentences/reddit")


def load_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_json(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def save_json(data: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def clean_text(text: str | None) -> str:
    if text is None:
        return ""

    text = str(text)
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = re.sub(r"\[deleted\]|\[removed\]", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def construct_keyword_regex(keywords: list[str]) -> re.Pattern:
    if not keywords:
        raise ValueError("No keywords found in config.yaml under sentence_preprocess.ai_keywords")

    escaped = [re.escape(k.strip()) for k in keywords if k.strip()]
    bounded = [rf"\b{k}\b" for k in escaped]

    pattern = r"(?i)" + "|".join(bounded)
    return re.compile(pattern)


def extract_keyword_sentences_with_context(
    record: dict,
    nlp,
    keyword_regex: re.Pattern,
    min_sentence_chars: int = 20,
) -> list[dict]:
    item_id = record.get("item_id")

    if not item_id:
        return []

    title = clean_text(record.get("title"))
    text = clean_text(record.get("text"))

    if title and text and not text.startswith(title):
        text_to_process = f"{title}. {text}"
    else:
        text_to_process = text or title

    if not text_to_process:
        return []

    try:
        doc = nlp(text_to_process)
    except Exception as exc:
        logging.warning("spaCy failed for item_id=%s | error=%s", item_id, exc)
        return []

    sentences = [clean_text(sent.text) for sent in doc.sents if clean_text(sent.text)]

    results = []

    for idx, sentence in enumerate(sentences):
        if len(sentence) < min_sentence_chars:
            continue

        if not keyword_regex.search(sentence):
            continue

        preceding_sentence = sentences[idx - 1] if idx > 0 else None
        subsequent_sentence = sentences[idx + 1] if idx < len(sentences) - 1 else None

        context_text = " ".join(
            s for s in [preceding_sentence, sentence, subsequent_sentence] if s
        )

        results.append(
            {
                "sentence_id": f"{item_id}_s{idx:04d}",
                "item_id": item_id,
                "item_type": record.get("item_type"),
                "source": record.get("source"),
                "subreddit": record.get("subreddit"),
                "author": record.get("author"),
                "created_utc": record.get("created_utc"),
                "sentence_index": idx,
                "preceding_sentence": preceding_sentence,
                "ai_sentence": sentence,
                "subsequent_sentence": subsequent_sentence,
                "context_text": context_text,
                "full_text": text_to_process,
                "score": record.get("score"),
                "num_comments": record.get("num_comments"),
                "url": record.get("url"),
                "link_id": record.get("link_id"),
                "parent_id": record.get("parent_id"),
            }
        )

    return results


def process_run_folder(
    run_folder: Path,
    nlp,
    keyword_regex: re.Pattern,
    min_sentence_chars: int,
) -> list[dict]:
    input_path = run_folder / "cleaned_data.json"

    if not input_path.exists():
        logging.warning("Skipping %s because cleaned_data.json is missing", run_folder)
        return []

    records = load_json(input_path)

    sentence_records: list[dict] = []

    for record in records:
        sentence_records.extend(
            extract_keyword_sentences_with_context(
                record=record,
                nlp=nlp,
                keyword_regex=keyword_regex,
                min_sentence_chars=min_sentence_chars,
            )
        )

    return sentence_records


def main() -> None:
    config = load_config(CONFIG_PATH)

    keywords = config.get("sentence_preprocess", {}).get("ai_keywords", [])

    logging.info("Loaded %s AI keywords", len(keywords))

    keyword_regex = construct_keyword_regex(keywords)

    sentence_config = config.get("sentence_preprocess", {})
    spacy_model = sentence_config.get("spacy_model", "en_core_web_sm")
    min_sentence_chars = int(sentence_config.get("min_sentence_chars", 20))

    logging.info("Loading spaCy model: %s", spacy_model)
    nlp = spacy.load(spacy_model, disable=["ner"])

    if not CLEAN_DIR.exists():
        raise FileNotFoundError("No cleaned data found. Run 02_clean_store.py first.")

    total_sentences = 0
    processed_folders = 0

    for run_folder in sorted(CLEAN_DIR.iterdir()):
        if not run_folder.is_dir():
            continue

        logging.info("Processing %s", run_folder.name)

        sentence_data = process_run_folder(
            run_folder=run_folder,
            nlp=nlp,
            keyword_regex=keyword_regex,
            min_sentence_chars=min_sentence_chars,
        )

        if not sentence_data:
            logging.warning("No AI-related sentences for %s", run_folder.name)
            continue

        output_path = SENTENCE_DIR / run_folder.name / "sentences.json"
        save_json(sentence_data, output_path)

        logging.info(
            "Saved %s | sentences=%s",
            run_folder.name,
            len(sentence_data),
        )

        total_sentences += len(sentence_data)
        processed_folders += 1

    logging.info(
        "DONE | folders=%s | total_sentences=%s",
        processed_folders,
        total_sentences,
    )


if __name__ == "__main__":
    main()