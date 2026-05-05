from __future__ import annotations

import argparse
import email
import imaplib
import logging
import os
import re
import smtplib
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable
TWILIO_BASE_URL = "https://api.twilio.com/2010-04-01"


def load_dotenv_file(path: Path) -> None:
    """Load simple KEY=VALUE pairs from a .env file into os.environ if unset."""
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")

            # Only set env var if not already present
            if os.environ.get(key) is None:
                os.environ[key] = val



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


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def parse_twilio_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %z").astimezone(
            timezone.utc
        )
    except ValueError:
        return None


def parse_approval_reply(message_body: str) -> bool | None:
    text = message_body or ""

    for raw_line in text.splitlines():
        line = raw_line.strip().lower()
        if not line:
            continue

        if line.startswith(">"):
            continue

        if line.startswith(("on ", "from:", "subject:", "to:", "sent:")):
            continue

        tokens = [token for token in re.split(r"[^a-z]+", line) if token]
        if not tokens:
            continue

        first_token = tokens[0]
        if first_token in {"y", "yes", "approve", "approved", "start"}:
            return True

        if first_token in {"n", "no", "deny", "decline", "declined", "skip", "stop"}:
            return False

    return None


def parse_email_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc)
    except Exception:
        return None


def extract_text_from_email(msg: email.message.Message) -> str:
    if msg.is_multipart():
        parts = []

        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue

            content_type = part.get_content_type()
            if content_type != "text/plain":
                continue

            payload = part.get_payload(decode=True)
            charset = part.get_content_charset() or "utf-8"

            if payload is None:
                continue

            parts.append(payload.decode(charset, errors="ignore"))

        return "\n".join(parts).strip()

    payload = msg.get_payload(decode=True)
    charset = msg.get_content_charset() or "utf-8"

    if payload is None:
        body = msg.get_payload()
        return str(body).strip() if body else ""

    return payload.decode(charset, errors="ignore").strip()


def send_email(
    smtp_host: str,
    smtp_port: int,
    username: str,
    password: str,
    from_email: str,
    to_email: str,
    subject: str,
    body: str,
) -> None:
    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as smtp:
        smtp.login(username, password)
        smtp.send_message(msg)


def wait_for_email_approval(
    imap_host: str,
    imap_port: int,
    username: str,
    password: str,
    approver_email: str,
    token: str,
    sent_at: datetime,
    timeout_minutes: int,
    poll_seconds: int,
) -> bool | None:
    deadline = time.time() + max(1, timeout_minutes) * 60

    while time.time() < deadline:
        with imaplib.IMAP4_SSL(imap_host, imap_port) as imap:
            imap.login(username, password)
            imap.select("INBOX")

            status, data = imap.search(None, "ALL")
            if status != "OK" or not data:
                time.sleep(max(5, poll_seconds))
                continue

            message_ids = data[0].split()

            for message_id in reversed(message_ids[-100:]):
                status, msg_data = imap.fetch(message_id, "(RFC822)")
                if status != "OK" or not msg_data:
                    continue

                raw_msg = msg_data[0][1]
                if raw_msg is None:
                    continue

                msg = email.message_from_bytes(raw_msg)

                from_header = str(msg.get("From", "")).lower()
                if approver_email.lower() not in from_header:
                    continue

                subject = str(msg.get("Subject", ""))
                if token not in subject:
                    continue

                created_at = parse_email_datetime(str(msg.get("Date", "")))
                if created_at is None or created_at < sent_at:
                    continue

                body = extract_text_from_email(msg)
                decision = parse_approval_reply(body)
                if decision is not None:
                    return decision

        time.sleep(max(5, poll_seconds))

    return None


def request_email_approval(timeout_minutes: int, poll_seconds: int) -> bool:
    smtp_host = get_required_env("FRAMESCOPE_EMAIL_SMTP_HOST")
    smtp_port = int(os.getenv("FRAMESCOPE_EMAIL_SMTP_PORT", "465"))
    imap_host = get_required_env("FRAMESCOPE_EMAIL_IMAP_HOST")
    imap_port = int(os.getenv("FRAMESCOPE_EMAIL_IMAP_PORT", "993"))
    email_username = get_required_env("FRAMESCOPE_EMAIL_USERNAME")
    email_password = get_required_env("FRAMESCOPE_EMAIL_PASSWORD")
    approver_email = get_required_env("FRAMESCOPE_APPROVER_EMAIL")
    from_email = os.getenv("FRAMESCOPE_EMAIL_FROM", email_username).strip() or email_username

    sent_at = datetime.now(timezone.utc)
    sent_at_iso = sent_at.strftime("%Y-%m-%d %H:%M UTC")
    token = uuid.uuid4().hex[:8].upper()
    subject = f"[FrameScope Approval {token}] Weekly run request"

    prompt_text = (
        "FrameScope weekly run is ready to start.\n\n"
        f"Requested at: {sent_at_iso}\n"
        f"Approval token: {token}\n"
        f"Expires in: {timeout_minutes} minute(s)\n\n"
        "Reply with a single line starting with Y to start or N to skip.\n"
        "Please keep the token in the subject line."
    )

    send_email(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        username=email_username,
        password=email_password,
        from_email=from_email,
        to_email=approver_email,
        subject=subject,
        body=prompt_text,
    )

    logging.info(
        "Email approval request sent to %s. Waiting up to %s minute(s).",
        approver_email,
        timeout_minutes,
    )

    decision = wait_for_email_approval(
        imap_host=imap_host,
        imap_port=imap_port,
        username=email_username,
        password=email_password,
        approver_email=approver_email,
        token=token,
        sent_at=sent_at,
        timeout_minutes=timeout_minutes,
        poll_seconds=poll_seconds,
    )

    if decision is None:
        send_email(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            username=email_username,
            password=email_password,
            from_email=from_email,
            to_email=approver_email,
            subject=f"[FrameScope Approval {token}] Weekly run skipped",
            body="FrameScope weekly run skipped: no Y/N email reply received before timeout.",
        )
        logging.warning("No email approval reply received before timeout.")
        return False

    if not decision:
        send_email(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            username=email_username,
            password=email_password,
            from_email=from_email,
            to_email=approver_email,
            subject=f"[FrameScope Approval {token}] Weekly run skipped",
            body="FrameScope weekly run skipped as requested (reply=N).",
        )
        logging.info("Weekly run declined via email reply.")
        return False

    send_email(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        username=email_username,
        password=email_password,
        from_email=from_email,
        to_email=approver_email,
        subject=f"[FrameScope Approval {token}] Weekly run approved",
        body="FrameScope weekly run approved. Starting now.",
    )
    logging.info("Weekly run approved via email reply.")
    return True


def twilio_request(
    account_sid: str,
    auth_token: str,
    method: str,
    resource: str,
    params: dict[str, str] | None = None,
    data: dict[str, str] | None = None,
) -> dict:
    url = f"{TWILIO_BASE_URL}/Accounts/{account_sid}/{resource}"

    response = requests.request(
        method=method,
        url=url,
        params=params,
        data=data,
        auth=(account_sid, auth_token),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def send_sms(
    account_sid: str,
    auth_token: str,
    from_number: str,
    to_number: str,
    body: str,
) -> str:
    payload = twilio_request(
        account_sid=account_sid,
        auth_token=auth_token,
        method="POST",
        resource="Messages.json",
        data={"From": from_number, "To": to_number, "Body": body},
    )
    return str(payload.get("sid", ""))


def wait_for_sms_approval(
    account_sid: str,
    auth_token: str,
    from_number: str,
    approver_number: str,
    sent_at: datetime,
    timeout_minutes: int,
    poll_seconds: int,
) -> bool | None:
    deadline = time.time() + max(1, timeout_minutes) * 60

    while time.time() < deadline:
        payload = twilio_request(
            account_sid=account_sid,
            auth_token=auth_token,
            method="GET",
            resource="Messages.json",
            params={"To": from_number, "From": approver_number, "PageSize": "50"},
        )

        messages = payload.get("messages", [])

        for msg in messages:
            direction = str(msg.get("direction", ""))
            if not direction.startswith("inbound"):
                continue

            created_at = parse_twilio_datetime(msg.get("date_created"))
            if created_at is None or created_at < sent_at:
                continue

            decision = parse_approval_reply(str(msg.get("body", "")))
            if decision is not None:
                return decision

        time.sleep(max(5, poll_seconds))

    return None


def request_sms_approval(timeout_minutes: int, poll_seconds: int) -> bool:
    account_sid = get_required_env("FRAMESCOPE_TWILIO_ACCOUNT_SID")
    auth_token = get_required_env("FRAMESCOPE_TWILIO_AUTH_TOKEN")
    from_number = get_required_env("FRAMESCOPE_TWILIO_FROM_NUMBER")
    approver_number = get_required_env("FRAMESCOPE_APPROVER_PHONE")

    sent_at = datetime.now(timezone.utc)
    sent_at_iso = sent_at.strftime("%Y-%m-%d %H:%M UTC")

    prompt_text = (
        "FrameScope weekly run is ready to start "
        f"({sent_at_iso}). Reply Y to start or N to skip. "
        f"This request expires in {timeout_minutes} minutes."
    )

    send_sms(
        account_sid=account_sid,
        auth_token=auth_token,
        from_number=from_number,
        to_number=approver_number,
        body=prompt_text,
    )

    logging.info(
        "SMS approval request sent to %s. Waiting up to %s minute(s).",
        approver_number,
        timeout_minutes,
    )

    decision = wait_for_sms_approval(
        account_sid=account_sid,
        auth_token=auth_token,
        from_number=from_number,
        approver_number=approver_number,
        sent_at=sent_at,
        timeout_minutes=timeout_minutes,
        poll_seconds=poll_seconds,
    )

    if decision is None:
        send_sms(
            account_sid=account_sid,
            auth_token=auth_token,
            from_number=from_number,
            to_number=approver_number,
            body="FrameScope weekly run skipped: no Y/N reply received before timeout.",
        )
        logging.warning("No SMS approval reply received before timeout.")
        return False

    if not decision:
        send_sms(
            account_sid=account_sid,
            auth_token=auth_token,
            from_number=from_number,
            to_number=approver_number,
            body="FrameScope weekly run skipped as requested (reply=N).",
        )
        logging.info("Weekly run declined via SMS reply.")
        return False

    send_sms(
        account_sid=account_sid,
        auth_token=auth_token,
        from_number=from_number,
        to_number=approver_number,
        body="FrameScope weekly run approved. Starting now.",
    )
    logging.info("Weekly run approved via SMS reply.")
    return True


def run_pipeline(
    skip_archive: bool,
    archive_dry_run: bool,
    skip_neon_upload: bool,
    sms_approval: bool,
    sms_timeout_minutes: int,
    sms_poll_seconds: int,
    email_approval: bool,
    email_timeout_minutes: int,
    email_poll_seconds: int,
    dry_run: bool,
 ) -> None:
    if dry_run:
        if email_approval:
            # Show simulated email content and return
            sent_at = datetime.now(timezone.utc)
            sent_at_iso = sent_at.strftime("%Y-%m-%d %H:%M UTC")
            token = "<SIMULATED-TOKEN>"
            subject = f"[FrameScope Approval {token}] Weekly run request"
            prompt_text = (
                "FrameScope weekly run (DRY RUN) would be sent.\n\n"
                f"Requested at: {sent_at_iso}\n"
                f"Approval token: {token}\n"
                f"Expires in: {email_timeout_minutes} minute(s)\n\n"
                "Reply with Y to start or N to skip.\n"
            )
            try:
                approver = get_required_env("FRAMESCOPE_APPROVER_EMAIL")
            except ValueError:
                approver = "<approver-email-not-set>"

            logging.info(
                "DRY RUN: Email approval would be sent to %s:\nSubject: %s\nBody:\n%s",
                approver,
                subject,
                prompt_text,
            )
            return

        if sms_approval:
            sent_at = datetime.now(timezone.utc)
            sent_at_iso = sent_at.strftime("%Y-%m-%d %H:%M UTC")
            prompt_text = (
                "FrameScope weekly run (DRY RUN) would be sent via SMS.\n"
                f"Requested at: {sent_at_iso}. Reply Y to start or N to skip."
            )
            try:
                approver_phone = get_required_env("FRAMESCOPE_APPROVER_PHONE")
            except ValueError:
                approver_phone = "<approver-phone-not-set>"

            logging.info("DRY RUN: SMS approval would be sent to %s:\n%s", approver_phone, prompt_text)
            return

    if email_approval:
        approved = request_email_approval(
            timeout_minutes=email_timeout_minutes,
            poll_seconds=email_poll_seconds,
        )
        if not approved:
            logging.info("Pipeline not started because email approval was not granted.")
            return

    if sms_approval:
        approved = request_sms_approval(
            timeout_minutes=sms_timeout_minutes,
            poll_seconds=sms_poll_seconds,
        )
        if not approved:
            logging.info("Pipeline not started because SMS approval was not granted.")
            return

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

    parser.add_argument(
        "--sms-approval",
        action="store_true",
        help="Send SMS approval prompt first and only run if reply is Y/YES.",
    )

    parser.add_argument(
        "--email-approval",
        action="store_true",
        help="Send email approval prompt first and only run if reply is Y/YES.",
    )

    parser.add_argument(
        "--sms-timeout-minutes",
        type=int,
        default=30,
        help="How long to wait for an SMS Y/N reply before skipping run.",
    )

    parser.add_argument(
        "--sms-poll-seconds",
        type=int,
        default=20,
        help="Polling interval (seconds) while waiting for SMS Y/N reply.",
    )

    parser.add_argument(
        "--email-timeout-minutes",
        type=int,
        default=60,
        help="How long to wait for an email Y/N reply before skipping run.",
    )

    parser.add_argument(
        "--email-poll-seconds",
        type=int,
        default=30,
        help="Polling interval (seconds) while waiting for email Y/N reply.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate approval prompt (email/SMS) and do not run pipeline.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Load .env from project root or workspace root if present so env vars in .env
    # are available to the approval flows without requiring manual export.
    dotenv_paths = [PROJECT_ROOT / ".env", Path(".env")]
    for p in dotenv_paths:
        load_dotenv_file(p)

    if args.sms_approval and args.email_approval:
        raise ValueError("Use only one approval mode at a time: --sms-approval or --email-approval.")

    try:
        run_pipeline(
            skip_archive=args.skip_archive,
            archive_dry_run=args.archive_dry_run,
            skip_neon_upload=args.skip_neon_upload,
            sms_approval=args.sms_approval,
            sms_timeout_minutes=args.sms_timeout_minutes,
            sms_poll_seconds=args.sms_poll_seconds,
            email_approval=args.email_approval,
            email_timeout_minutes=args.email_timeout_minutes,
            email_poll_seconds=args.email_poll_seconds,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        logging.exception("Weekly pipeline failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()