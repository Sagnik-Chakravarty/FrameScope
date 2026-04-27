from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable


PIPELINE_STEPS = [
    ("fetch_reddit", ["Scripts/01_fetch_reddit.py"]),
    ("clean_store", ["Scripts/02_clean_store.py"]),
    ("sentence_preprocess", ["Scripts/03_sentence_preprocess.py"]),
    ("update_database", ["Scripts/04_update_database.py"]),
    ("label_llm", ["Scripts/05_label_llm.py"]),
    ("weekly_aggregate", ["Scripts/06_weekly_aggregate.py"]),
    ("weekly_llm_summary", ["Scripts/07_weekly_llm_summary.py"]),
    ("monthly_llm_summary", ["Scripts/08_monthly_llm_summary.py"]),
    ("yearly_llm_summary", ["Scripts/09_yearly_llm_summary.py"]),
    ("volume_shift_month", ["Scripts/10_volume_shift_summary.py", "--period-type", "month"]),
    ("volume_shift_year", ["Scripts/10_volume_shift_summary.py", "--period-type", "year"]),
]


def run_step(step_name: str, command: list[str]) -> None:
    full_command = [PYTHON, *command]

    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(PROJECT_ROOT)
        if not existing_pythonpath
        else f"{PROJECT_ROOT}:{existing_pythonpath}"
    )

    logging.info("Starting step: %s", step_name)
    logging.info("Command: %s", " ".join(full_command))

    result = subprocess.run(
        full_command,
        cwd=PROJECT_ROOT,
        text=True,
        env=env,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Pipeline failed at step '{step_name}' with exit code {result.returncode}."
        )

    logging.info("Completed step: %s", step_name)


def run_pipeline(
    skip_archive: bool,
    archive_dry_run: bool,
    skip_neon_upload: bool,
) -> None:
    started_at = datetime.now(timezone.utc)

    logging.info("FrameScope weekly pipeline started at %s", started_at.isoformat())

    for step_name, command in PIPELINE_STEPS:
        run_step(step_name, command)

    if skip_archive:
        logging.info("Skipping archive step because --skip-archive was used.")
    else:
        archive_command = ["Scripts/11_archive_and_prune.py"]

        if archive_dry_run:
            archive_command.append("--dry-run")

        run_step("archive_and_prune", archive_command)

    if skip_neon_upload:
        logging.info("Skipping Neon upload because --skip-neon-upload was used.")
    elif archive_dry_run:
        logging.info("Skipping Neon upload because archive was run in dry-run mode.")
    elif skip_archive:
        logging.info("Skipping Neon upload because archive step was skipped.")
    else:
        run_step(
            "upload_aggregate_to_neon",
            ["Scripts/12_upload_aggregate_to_neon.py"],
        )

    finished_at = datetime.now(timezone.utc)
    logging.info("FrameScope weekly pipeline finished at %s", finished_at.isoformat())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full weekly FrameScope Reddit pipeline."
    )

    parser.add_argument(
        "--skip-archive",
        action="store_true",
        help="Run the full pipeline but skip archive/prune.",
    )

    parser.add_argument(
        "--archive-dry-run",
        action="store_true",
        help="Run archive step in dry-run mode.",
    )

    parser.add_argument(
        "--skip-neon-upload",
        action="store_true",
        help="Run the full pipeline but skip uploading aggregate tables to Neon.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        run_pipeline(
            skip_archive=args.skip_archive,
            archive_dry_run=args.archive_dry_run,
            skip_neon_upload=args.skip_neon_upload,
        )
    except Exception as exc:
        logging.exception("Weekly pipeline failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()