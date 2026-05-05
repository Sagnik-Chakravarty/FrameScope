# FrameScope

FrameScope is a reproducible research pipeline for analyzing how people describe AI in real-world text.

The project collects Reddit data, normalizes and stores it, labels it with LLMs, and then produces weekly, monthly, yearly, and volume-shift summaries. It also includes an Arctic Shift client package, `arcshiftwrap`, for direct data collection from the Arctic Shift API.

## Contents

- [FrameScope](#framescope)
  - [Contents](#contents)
  - [Overview](#overview)
  - [Installation](#installation)
  - [Benchmark Results](#benchmark-results)
    - [Overall performance](#overall-performance)
    - [Key takeaways](#key-takeaways)
    - [Latency and cost](#latency-and-cost)
    - [Human baseline comparison](#human-baseline-comparison)
  - [Pipeline](#pipeline)
  - [Directory Structure](#directory-structure)
  - [Setup](#setup)
    - [1. Prepare Ollama models](#1-prepare-ollama-models)
    - [2. Verify Ollama is running](#2-verify-ollama-is-running)
    - [3. Notes](#3-notes)

## Overview

FrameScope is designed to answer a practical question: how well do different models capture the language people actually use to talk about AI?

The repository combines:

- Reddit ingestion through Arctic Shift
- Cleaning and sentence-level preprocessing
- LLM-based metaphor and stance labeling
- Weekly, monthly, yearly, and shift summaries
- Benchmarking of GPT and local models against human annotations

## Installation

Install the client package from PyPI:

```bash
pip install arcshiftwrap
```

Example usage:

```python
from arcshiftwrap import ArcticShiftClient

client = ArcticShiftClient()
posts = client.search_posts(
    subreddit="MachineLearning",
    after="2024-01-01",
)
```

To work with the full repository locally:

```bash
git clone https://github.com/sagnik-chakravarty/FrameScope.git
cd FrameScope
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

## Benchmark Results

The LLM comparison notebook evaluates models on 4,147 labeled examples and reports classification performance using accuracy, macro F1, weighted F1, macro precision, macro recall, and Cohen's kappa.

### Overall performance

| Model | Accuracy | Macro F1 | Weighted F1 | Notes |
| --- | ---: | ---: | ---: | --- |
| gpt-4o-mini | 0.7485 | 0.6717 | 0.7582 | Best overall Macro F1 in the benchmark |
| gpt-4.1-mini | 0.7311 | 0.6410 | 0.7381 | Strong low-cost GPT option |
| gpt-4.1-nano | 0.7335 | 0.6397 | 0.7401 | Very competitive for its cost |
| llama3.1:8b | 0.7333 | 0.6412 | 0.7362 | Best local model by Macro F1 |
| llama3.2 | 0.6639 | 0.5948 | 0.6753 | Fastest local model in the latency benchmark |

### Key takeaways

- `gpt-4o-mini` is the strongest model overall on raw classification quality.
- `gpt-4.1-nano` and `gpt-4.1-mini` are the best GPT options when cost matters.
- `llama3.1:8b` is the strongest local model on this benchmark.
- `llama3.2` is the most operationally efficient local option, with the lowest measured latency in the latency-cost comparison.

### Latency and cost

The latency-cost comparison shows the practical trade-off between API-based and local models.

- `gpt-4.1-nano` recorded the lowest GPT cost in the comparison at about `$0.1307` total.
- `gpt-4o-mini` delivered the best overall quality/cost balance among GPT models.
- `llama3.2` had the lowest local average latency at about `0.47s` per item.

### Human baseline comparison

The older prompt benchmark in `gpt41_old_prompt_overall_results.csv` shows the value of the updated prompt and pipeline:

- `gpt-4.1_old_prompt` macro F1: `0.3148`
- Updated benchmark models now reach roughly `0.59` to `0.67` macro F1 depending on the model

## Pipeline

The weekly pipeline is orchestrated by `Scripts/13_run_weekly_pipeline.py` and runs the processing stages in order.

| Step | Script | Purpose |
| --- | --- | --- |
| 00 | `Scripts/00_backfill_reddit.py` | Backfill Reddit data from Arctic Shift into the raw store |
| 01 | `Scripts/01_fetch_reddit.py` | Fetch new Reddit data for the current run |
| 02 | `Scripts/02_clean_store.py` | Clean and normalize raw records into the processed layer |
| 03 | `Scripts/03_sentence_preprocess.py` | Split text into sentence-level records and prepare downstream inputs |
| 04 | `Scripts/04_update_database.py` | Load cleaned and processed data into the SQLite database |
| 05 | `Scripts/05_label_llm.py` | Apply LLM labels such as metaphor and stance |
| 06 | `Scripts/06_weekly_aggregate.py` | Build weekly aggregate summaries and example selections |
| 07 | `Scripts/07_weekly_llm_summary.py` | Generate weekly LLM summaries |
| 08 | `Scripts/08_monthly_llm_summary.py` | Generate monthly LLM summaries from weekly outputs |
| 09 | `Scripts/09_yearly_llm_summary.py` | Generate yearly LLM summaries from monthly outputs |
| 10 | `Scripts/10_volume_shift_summary.py` | Summarize month-over-month or year-over-year volume shifts |
| 11 | `Scripts/11_archive_and_prune.py` | Archive old data and prune working storage |
| 12 | `Scripts/12_upload_aggregate_to_neon.py` | Upload aggregate tables to Neon |
| 13 | `Scripts/13_run_weekly_pipeline.py` | Run the full pipeline end-to-end |

Run the full weekly pipeline with:

```bash
python3 Scripts/13_run_weekly_pipeline.py
```

Useful options:

```bash
python3 Scripts/13_run_weekly_pipeline.py --skip-archive
python3 Scripts/13_run_weekly_pipeline.py --archive-dry-run
python3 Scripts/13_run_weekly_pipeline.py --skip-neon-upload
```

Email approval gate (free):

```bash
export FRAMESCOPE_EMAIL_SMTP_HOST="smtp.gmail.com"
export FRAMESCOPE_EMAIL_SMTP_PORT="465"
export FRAMESCOPE_EMAIL_IMAP_HOST="imap.gmail.com"
export FRAMESCOPE_EMAIL_IMAP_PORT="993"
export FRAMESCOPE_EMAIL_USERNAME="you@example.com"
export FRAMESCOPE_EMAIL_PASSWORD="your_app_password"
export FRAMESCOPE_EMAIL_FROM="you@example.com"
export FRAMESCOPE_APPROVER_EMAIL="you@example.com"

python3 Scripts/13_run_weekly_pipeline.py --email-approval
```

When `--email-approval` is used, the script sends an approval email and waits for a reply:

- Reply `Y` or `YES` to start the run.
- Reply `N` or `NO` to skip the run.
- If no reply arrives before timeout, the run is skipped.

Optional email tuning:

```bash
python3 Scripts/13_run_weekly_pipeline.py \
  --email-approval \
  --email-timeout-minutes 90 \
  --email-poll-seconds 20
```

SMS approval gate (Twilio):

```bash
export FRAMESCOPE_TWILIO_ACCOUNT_SID="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
export FRAMESCOPE_TWILIO_AUTH_TOKEN="your_twilio_auth_token"
export FRAMESCOPE_TWILIO_FROM_NUMBER="+1XXXXXXXXXX"
export FRAMESCOPE_APPROVER_PHONE="+1YYYYYYYYYY"

python3 Scripts/13_run_weekly_pipeline.py --sms-approval
```

When `--sms-approval` is used, the script sends an SMS prompt and waits for a reply:

- Reply `Y` or `YES` to start the run.
- Reply `N` or `NO` to skip the run.
- If no reply arrives before timeout, the run is skipped.

Optional SMS tuning:

```bash
python3 Scripts/13_run_weekly_pipeline.py \
  --sms-approval \
  --sms-timeout-minutes 45 \
  --sms-poll-seconds 15
```

### Weekly Automation on macOS (launchd)

1. Create a wrapper script, for example `~/bin/framescope_weekly.sh`:

```bash
#!/bin/zsh
set -euo pipefail

cd /Volumes/SSD500GB/FrameScope

export FRAMESCOPE_TWILIO_ACCOUNT_SID="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
export FRAMESCOPE_TWILIO_AUTH_TOKEN="your_twilio_auth_token"
export FRAMESCOPE_TWILIO_FROM_NUMBER="+1XXXXXXXXXX"
export FRAMESCOPE_APPROVER_PHONE="+1YYYYYYYYYY"

export FRAMESCOPE_EMAIL_SMTP_HOST="smtp.gmail.com"
export FRAMESCOPE_EMAIL_SMTP_PORT="465"
export FRAMESCOPE_EMAIL_IMAP_HOST="imap.gmail.com"
export FRAMESCOPE_EMAIL_IMAP_PORT="993"
export FRAMESCOPE_EMAIL_USERNAME="you@example.com"
export FRAMESCOPE_EMAIL_PASSWORD="your_app_password"
export FRAMESCOPE_EMAIL_FROM="you@example.com"
export FRAMESCOPE_APPROVER_EMAIL="you@example.com"

/usr/bin/python3 Scripts/13_run_weekly_pipeline.py --email-approval
```

2. Make it executable:

```bash
chmod +x ~/bin/framescope_weekly.sh
```

3. Create `~/Library/LaunchAgents/com.framescope.weekly.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.framescope.weekly</string>

    <key>ProgramArguments</key>
    <array>
      <string>/bin/zsh</string>
      <string>/Users/your-user/bin/framescope_weekly.sh</string>
    </array>

    <key>StartCalendarInterval</key>
    <dict>
      <key>Weekday</key>
      <integer>1</integer>
      <key>Hour</key>
      <integer>9</integer>
      <key>Minute</key>
      <integer>0</integer>
    </dict>

    <key>WorkingDirectory</key>
    <string>/Volumes/SSD500GB/FrameScope</string>

    <key>StandardOutPath</key>
    <string>/tmp/framescope_weekly.out</string>
    <key>StandardErrorPath</key>
    <string>/tmp/framescope_weekly.err</string>

    <key>RunAtLoad</key>
    <false/>
  </dict>
</plist>
```

4. Load and start the schedule:

```bash
launchctl unload ~/Library/LaunchAgents/com.framescope.weekly.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.framescope.weekly.plist
launchctl start com.framescope.weekly
```

The scheduled run now asks by email first. It starts only when you reply `Y`.

Tip for Gmail users: use an App Password (with 2FA enabled) instead of your normal login password.

## Directory Structure

| Path | Purpose |
| --- | --- |
| `arcshiftwrap/` | Installable Arctic Shift client package |
| `Scripts/` | End-to-end pipeline stages and maintenance jobs |
| `Notebooks/` | Benchmarking, analysis, and debugging notebooks |
| `Docs/` | Project documentation and API notes |
| `LLM results/` | Shareable benchmark inputs, outputs, evaluation CSVs, and GPT batch artifacts |
| `plots/` | Generated charts and benchmark figures |
| `Prompts/` | Prompt templates and label schema files |
| `Setup/` | Environment bootstrap scripts and model installers |

Generated data, database files, and other run artifacts are intentionally omitted from this overview because they are created locally and excluded from version control. The top-level `LLM results/` directory is kept in version control so benchmark inputs and outputs remain easy to access.

## Setup

### 1. Prepare Ollama models

This helper script installs Ollama if needed, starts the service when possible, and downloads any missing local models:

```bash
python Setup/download_ollama_models.py
```

### 2. Verify Ollama is running

```bash
curl http://localhost:11434/api/tags
```

If Ollama is working, you should see a JSON response.

### 3. Notes

- If you see `connection refused`, start Ollama with `ollama serve`.
- The downloader skips models that are already installed.
- Large local models such as `llama3.1:70b` may require significant RAM and disk space.
