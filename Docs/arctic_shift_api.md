from pathlib import Path

# Arctic Shift API Usage Documentation for FrameScope

## Overview

FrameScope uses the Arctic Shift API to collect Reddit posts and comments from selected subreddits. The collected data is used for downstream analysis of AI-related discourse, including keyword filtering, metaphor framing detection, stance labeling, and dashboard visualization.

Arctic Shift is used only as the Reddit data collection layer. The API output is saved locally first, then cleaned, filtered, stored, and labeled in later pipeline stages.

Base URL:

https://arctic-shift.photon-reddit.com

Status page:

https://status.arctic-shift.photon-reddit.com

Search UI:

https://arctic-shift.photon-reddit.com/search


## Why FrameScope Uses Arctic Shift

FrameScope needs historical and recent Reddit data from multiple subreddits. Arctic Shift is useful because it supports:

- subreddit-based post search
- subreddit-based comment search
- date-window filtering
- selected-field retrieval
- post/comment ID lookup
- comment tree retrieval
- aggregation endpoints
- subreddit metadata endpoints
- user metadata and interaction endpoints
- time-series endpoints

For the first version of FrameScope, the main endpoints used are:

- /api/posts/search
- /api/comments/search

Other endpoints are wrapped in the client for future extensions.


## Current FrameScope Collection Strategy

FrameScope follows this collection design:

1. Read subreddits, keywords, and API settings from config.yaml.
2. Compute a dynamic collection window.
3. Fetch posts and comments from Arctic Shift by subreddit and date range.
4. Split large time ranges into smaller windows.
5. Save raw API output.
6. Apply local keyword filtering.
7. Save filtered output separately.
8. Later scripts clean/store the data and run LLM labeling.

The preferred design is:

fetch broadly -> save raw -> filter locally -> clean/store -> label with LLM -> aggregate -> dashboard


## Why FrameScope Does Not Rely Primarily on API Keyword Search

Arctic Shift supports keyword parameters such as:

- title
- selftext
- query
- body

However, these can be fragile for large-scale collection. The API documentation notes that keyword search can timeout, especially with large or active subreddits.

Therefore, FrameScope uses this safer strategy:

- Fetch posts/comments by subreddit and date range.
- Save all fetched records.
- Apply local keyword filtering in Python.

This makes the pipeline more reproducible and easier to debug.


## Main API Endpoints Used

### 1. Post Search

Endpoint:

/api/posts/search

Purpose:

Collect Reddit submissions from a subreddit within a date range.

Important parameters:

- subreddit: subreddit name, for example "technology"
- after: start date/time
- before: end date/time
- limit: number of records returned per request
- sort: asc or desc, sorted by created_utc
- fields: comma-separated list of fields to return
- query: optional keyword search over title and selftext
- title: optional title keyword search
- selftext: optional selftext keyword search
- author: optional author filter
- over_18: optional NSFW filter
- spoiler: optional spoiler filter
- link_flair_text: optional flair filter

Example API URL:

https://arctic-shift.photon-reddit.com/api/posts/search?subreddit=technology&after=2026-04-01&before=2026-04-08&limit=100&sort=asc

Example Python usage:

from framescope.arctic_shift import ArcticShiftClient

client = ArcticShiftClient()

posts = client.search_posts(
    subreddit="technology",
    after="2026-04-01",
    before="2026-04-08",
    limit=100,
    sort="asc",
    fields=[
        "id",
        "subreddit",
        "author",
        "created_utc",
        "title",
        "selftext",
        "url",
        "score",
        "num_comments"
    ],
)

print(len(posts))


### 2. Comment Search

Endpoint:

/api/comments/search

Purpose:

Collect Reddit comments from a subreddit within a date range.

Important parameters:

- subreddit: subreddit name
- after: start date/time
- before: end date/time
- limit: number of records returned per request
- sort: asc or desc, sorted by created_utc
- fields: comma-separated list of fields to return
- body: optional keyword search in comment body
- author: optional author filter
- link_id: optional post ID filter
- parent_id: optional parent comment filter

Example API URL:

https://arctic-shift.photon-reddit.com/api/comments/search?subreddit=technology&after=2026-04-01&before=2026-04-08&limit=100&sort=asc

Example Python usage:

comments = client.search_comments(
    subreddit="technology",
    after="2026-04-01",
    before="2026-04-08",
    limit=100,
    sort="asc",
    fields=[
        "id",
        "subreddit",
        "author",
        "created_utc",
        "body",
        "score",
        "link_id",
        "parent_id"
    ],
)

print(len(comments))


## Configuration File

FrameScope stores collection settings in config.yaml.

Example config.yaml:

reddit:
  subreddits:
    - news
    - politics
    - worldpolitics
    - cryptocurrency
    - technology
    - askreddit
    - moviecritic
    - art
    - artistlounge
    - nostupidquestions
    - antiwork

  keywords:
    - "AI"
    - "Deep Fake"
    - "Artificial Intelligence"
    - "Chat GPT"
    - "Open AI"
    - "LLAMA"
    - "Claude"
    - "Anthropic"
    - "Sora"
    - "Sam Altman"
    - "language model"
    - "Machine Learning"

  lookback_days: 7
  freshness_lag_hours: 48
  step_hours: 24
  limit: 100

api:
  sleep_seconds: 1.0
  max_retries: 4
  timeout: 90


## Explanation of Config Settings

### subreddits

The subreddits to collect from.

Example:

subreddits:
  - technology
  - askreddit
  - politics

The script automatically removes r/ if included.

For example:

r/technology

becomes:

technology


### keywords

Keywords used for local filtering after raw API data is fetched.

Example:

keywords:
  - "AI"
  - "Artificial Intelligence"
  - "Chat GPT"

FrameScope searches these terms in:

- post title
- post selftext
- comment body

The current keyword matching is exact phrase matching after lowercasing. For example, "Chat GPT" will match "chat gpt", but not necessarily "ChatGPT" unless "ChatGPT" is also included as a keyword.


### lookback_days

The number of days to fetch in each scheduled run.

Example:

lookback_days: 7

This means the pipeline collects one week of data per run.


### freshness_lag_hours

The delay between the present time and the most recent data collected.

Example:

freshness_lag_hours: 48

This means that if the pipeline runs on April 23, it collects data only up to roughly April 21.

Reason:

Arctic Shift notes that recent score and comment-count metadata may be incomplete before roughly 36 hours. A 48-hour lag protects against unstable early metrics.


### step_hours

The size of each time window used when querying Arctic Shift.

Example:

step_hours: 24

This splits a 7-day collection into seven 24-hour API windows.

For active subreddits, reduce this:

step_hours: 12

or:

step_hours: 6

Smaller windows reduce timeout and truncation risk.


### limit

The number of records requested per API call.

Example:

limit: 100

The API supports 1 to 100, or "auto". FrameScope uses 100 for deterministic behavior.

Avoid using "auto" for research workflows because the number of returned records may vary depending on server capacity.


### sleep_seconds

Seconds to wait between API calls.

Example:

sleep_seconds: 1.0

This is used to avoid hitting the free API too aggressively.


### max_retries

Number of times to retry a failed request.

Example:

max_retries: 4


### timeout

Request timeout in seconds.

Example:

timeout: 90


## Raw and Processed Output

FrameScope saves two versions of fetched data.

### Raw data

Saved to:

data/raw/reddit/<run_date>/

Example:

data/raw/reddit/2026-04-23/technology_posts.json
data/raw/reddit/2026-04-23/technology_comments.json

Raw data contains the API response after field selection but before keyword filtering.

Raw data should not be edited manually.

Purpose:

- reproducibility
- debugging
- reprocessing
- audit trail


### Processed data

Saved to:

data/processed/reddit/<run_date>/

Example:

data/processed/reddit/2026-04-23/technology_posts_filtered.json
data/processed/reddit/2026-04-23/technology_comments_filtered.json

Processed data contains locally keyword-filtered records.

This is the data that later scripts will clean, normalize, store in SQL, and label with LLMs.


## Example Workflow

### Step 1: Update config.yaml

Set the subreddits and keywords:

reddit:
  subreddits:
    - technology
    - askreddit

  keywords:
    - "AI"
    - "Artificial Intelligence"
    - "Machine Learning"

  lookback_days: 7
  freshness_lag_hours: 48
  step_hours: 24
  limit: 100


### Step 2: Run fetch script

From repo root:

python scripts/01_fetch_reddit.py


### Step 3: Check output folders

Expected output:

data/raw/reddit/<run_date>/
data/processed/reddit/<run_date>/


### Step 4: Inspect counts in terminal

Example log output:

Starting Reddit fetch
Window: 2026-04-14 to 2026-04-21
Subreddits: technology, askreddit
Keywords: AI, Artificial Intelligence, Machine Learning
Fetching r/technology
Saved r/technology | raw_posts=423 | filtered_posts=18 | raw_comments=700 | filtered_comments=41


## Example Use Cases

### Use Case 1: Weekly AI Discourse Monitoring

Goal:

Track how AI-related discourse changes across Reddit each week.

Subreddits:

- news
- technology
- politics
- askreddit

Keywords:

- AI
- Artificial Intelligence
- Chat GPT
- Open AI
- Machine Learning

Pipeline behavior:

1. Fetch the last 7 days of Reddit data.
2. Filter posts/comments mentioning AI-related keywords.
3. Store relevant posts/comments.
4. Run metaphor and stance labeling.
5. Update Tableau dashboard.

Dashboard outputs:

- number of AI-related posts by week
- metaphor categories over time
- stance distribution over time
- subreddit comparison


### Use Case 2: Deepfake Discourse Tracking

Goal:

Track how users discuss deepfakes and synthetic media.

Keywords:

- Deep Fake
- deepfake
- Sora
- synthetic media
- AI video

Subreddits:

- news
- technology
- politics
- moviecritic

Potential dashboard questions:

- Are deepfake discussions mostly political, technological, or entertainment-related?
- Which metaphors dominate deepfake discourse?
- Is stance mostly concerned, neutral, or positive?


### Use Case 3: AI Company and Figure Monitoring

Goal:

Monitor discourse around specific AI companies and public figures.

Keywords:

- Open AI
- Anthropic
- Claude
- Sam Altman
- LLAMA

Subreddits:

- technology
- news
- cryptocurrency
- askreddit

Possible analysis:

- comparison of public stance toward OpenAI vs Anthropic
- spikes in mentions around news events
- metaphor frames used for AI leaders and companies


### Use Case 4: Research Dataset Construction

Goal:

Build a reproducible dataset for a paper or class project.

Approach:

1. Define collection window through lookback_days and freshness_lag_hours.
2. Save raw API data.
3. Save filtered data.
4. Document all configuration settings.
5. Use raw data as an audit trail.

This supports reproducibility because the original API responses are preserved before filtering and labeling.


### Use Case 5: Tableau Dashboard Refresh

Goal:

Maintain a dashboard that updates automatically.

Workflow:

1. GitHub Actions or cron runs the fetch script weekly.
2. Data is stored locally or in cloud PostgreSQL.
3. Cleaning script converts JSON to SQL tables.
4. LLM script labels metaphor and stance.
5. SQL views aggregate weekly summaries.
6. Tableau connects to summary tables or extracts.

Recommended dashboard tabs:

- Overview
- Metaphor Trends
- Stance Trends
- Subreddit Comparison
- Example Texts


## Known API Limitations and FrameScope Responses

### Limitation 1: No uptime or performance guarantees

Arctic Shift is a free public service and may fail or slow down.

FrameScope response:

- retries failed requests
- logs errors
- uses sleep between requests
- avoids overly aggressive request patterns


### Limitation 2: No traditional pagination

Arctic Shift does not provide page tokens or cursors for normal search endpoints.

FrameScope response:

- splits the collection period into smaller time windows
- deduplicates records by ID
- uses sorted ascending date collection


### Limitation 3: Active subreddits may timeout

Large subreddits can produce query timeouts.

FrameScope response:

- default step_hours is 24
- reduce step_hours to 12 or 6 for active subreddits
- avoid unnecessary API-side keyword filters


### Limitation 4: Recent scores and comment counts may be unstable

Very recent posts may initially have score=1 or num_comments=0.

FrameScope response:

- uses freshness_lag_hours: 48
- avoids collecting the most recent 48 hours for analytical use


### Limitation 5: API keyword search may be inconsistent

Post keyword search and comment full-text search may not behave identically.

FrameScope response:

- fetch by subreddit and date first
- filter locally using consistent Python logic


### Limitation 6: Keyword filtering can miss semantic matches

Exact local keyword matching may miss related phrases.

Example:

Keyword list contains:

"Machine Learning"

But text says:

"ML systems are changing hiring."

This may not be matched.

FrameScope response:

- later LLM filtering can improve semantic recall
- keyword list should include common variants such as "ML", "ChatGPT", and "deepfake"


## Recommended Keyword Improvements

Current keyword list:

- AI
- Deep Fake
- Artificial Intelligence
- Chat GPT
- Open AI
- LLAMA
- Claude
- Anthropic
- Sora
- Sam Altman
- language model
- Machine Learning

Recommended additions:

- ChatGPT
- OpenAI
- LLM
- LLMs
- generative AI
- GenAI
- deepfake
- deepfakes
- machine learning
- ML
- large language model
- large language models
- artificial general intelligence
- AGI

Reason:

Some terms are commonly written without spaces, such as ChatGPT and OpenAI.


## Recommended Fields

### Posts

FrameScope currently collects:

- id
- subreddit
- author
- author_fullname
- author_flair_text
- created_utc
- retrieved_on
- title
- selftext
- url
- score
- num_comments
- link_flair_text
- over_18
- spoiler
- post_hint

These fields support:

- text analysis
- source tracking
- engagement analysis
- filtering
- deduplication


### Comments

FrameScope currently collects:

- id
- subreddit
- author
- author_fullname
- author_flair_text
- created_utc
- retrieved_on
- body
- score
- link_id
- parent_id

These fields support:

- comment-level discourse analysis
- linking comments to posts
- thread reconstruction
- engagement analysis


## Arctic Shift Client Design

The FrameScope Arctic Shift client is located at:

framescope/arctic_shift.py

It provides:

- ArcticShiftClient class
- request handling
- rate limit handling
- retry logic
- endpoint wrappers
- time-window splitting helpers
- response normalization
- deduplication helpers
- post collection by windows
- comment collection by windows


## Main Client Methods

### search_posts()

Fetches posts.

Example:

client.search_posts(
    subreddit="technology",
    after="2026-04-01",
    before="2026-04-08",
    limit=100,
    sort="asc"
)


### search_comments()

Fetches comments.

Example:

client.search_comments(
    subreddit="technology",
    after="2026-04-01",
    before="2026-04-08",
    limit=100,
    sort="asc"
)


### collect_posts_by_windows()

Fetches posts across many smaller time windows.

Example:

posts = collect_posts_by_windows(
    client=client,
    subreddit="technology",
    start=start_time,
    end=end_time,
    step_hours=24,
    limit=100,
    fields=POST_FIELDS,
)


### collect_comments_by_windows()

Fetches comments across many smaller time windows.

Example:

comments = collect_comments_by_windows(
    client=client,
    subreddit="technology",
    start=start_time,
    end=end_time,
    step_hours=24,
    limit=100,
    fields=COMMENT_FIELDS,
)


### get_comment_tree()

Retrieves a tree of comments under a specific post.

Example use case:

Use this when a post is especially important and you want the full conversation structure.


### aggregate_posts() and aggregate_comments()

Useful for summary counts without downloading all records.

Example use case:

Estimate how many AI-related comments appeared per month in a subreddit before deciding whether to download the full data.


### time_series()

Useful for broad activity summaries.

Example use case:

Track total number of posts in r/technology over time.


## Troubleshooting

### Error: ModuleNotFoundError: No module named 'framescope'

Cause:

Python cannot find the local package.

Fix:

Run from repo root:

python scripts/01_fetch_reddit.py

If needed:

export PYTHONPATH=.


### Error: Missing config file

Cause:

config.yaml is not in the repo root.

Fix:

Move config.yaml to the root directory.


### Empty raw files

Possible causes:

- subreddit had little activity in that window
- time window too narrow
- API returned no records
- field filters are too restrictive

Fix:

- increase lookback_days
- check the subreddit name
- test the API URL manually in browser


### Empty processed files

Possible causes:

- raw data was collected, but no records matched keywords
- keyword variants are missing

Fix:

Add variants such as:

- ChatGPT
- OpenAI
- LLM
- GenAI
- deepfake


### Query timeout

Possible causes:

- subreddit is too active
- window is too large
- keyword search was used at API level

Fix:

- reduce step_hours to 12 or 6
- avoid API-side keyword search
- retry later


### Very slow run

Possible causes:

- too many subreddits
- too small step_hours
- too many retries
- API is slow

Fix:

- test with 1 or 2 subreddits first
- use step_hours: 24 for initial testing
- reduce subreddit list during development


## Development Testing Recommendation

Before running the full pipeline, test with:

reddit:
  subreddits:
    - technology

  keywords:
    - "AI"
    - "ChatGPT"

  lookback_days: 2
  freshness_lag_hours: 48
  step_hours: 24
  limit: 100

This makes debugging faster.

After the test works, restore the full subreddit list.


