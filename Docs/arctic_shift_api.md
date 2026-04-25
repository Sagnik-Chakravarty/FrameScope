# Arctic Shift API Client Reference

This page gives practical examples for every public function in `arcshiftwrap/arctic_shift.py`.

- API Base: `https://arctic-shift.photon-reddit.com`
- Install: `pip install arcshiftwrap`

## Setup

```python
from datetime import datetime, timezone

from arcshiftwrap import ArcticShiftClient
from arcshiftwrap.arctic_shift import (
	collect_comments_by_windows,
	collect_posts_by_windows,
	deduplicate_items,
	format_date,
	normalize_response,
	split_time_range,
	utc_now,
)

client = ArcticShiftClient(timeout=90, max_retries=4, backoff_factor=2.0)
```


### `request`

Low-level method to send a GET request to the Arctic Shift API.

```python
response = client.request(
        endpoint="posts/search",
        params={"subreddit": "technology", "limit": 10}
)
```

## ID Lookup Methods

### `get_posts_by_ids`

```python
posts = client.get_posts_by_ids(
	ids=["1abcde", "1fghij"],
	fields=["id", "subreddit", "title", "created_utc"],
)
```

### `get_comments_by_ids`

```python
comments = client.get_comments_by_ids(
	ids=["k12345", "k67890"],
	fields=["id", "subreddit", "body", "created_utc"],
)
```

### `get_subreddits_by_ids`

```python
subreddits = client.get_subreddits_by_ids(
	ids=["technology", "MachineLearning"],
	fields=["display_name", "subscribers", "over18"],
)
```

### `get_users_by_ids`

```python
users = client.get_users_by_ids(
	ids=["spez", "AutoModerator"],
	fields=["author", "total_karma", "created_utc"],
)
```

## Search Methods

### `search_posts`

```python
posts = client.search_posts(
	subreddit="technology",
	after="2026-04-01",
	before="2026-04-08",
	limit=100,
	sort="asc",
	query="llm",
	fields=["id", "title", "selftext", "score", "num_comments"],
)
```

### `search_comments`

```python
comments = client.search_comments(
	subreddit="technology",
	after="2026-04-01",
	before="2026-04-08",
	limit=100,
	sort="asc",
	body="chatgpt",
	fields=["id", "body", "score", "link_id", "parent_id"],
)
```

### `get_comment_tree`

```python
tree = client.get_comment_tree(
	link_id="t3_1abcde",
	limit=500,
	start_breadth=4,
	start_depth=4,
)
```

## Aggregation Methods

### `aggregate_posts`

```python
post_agg = client.aggregate_posts(
	aggregate="subreddit",
	frequency="day",
	after="2026-04-01",
	before="2026-04-08",
	min_count=5,
)
```

### `aggregate_comments`

```python
comment_agg = client.aggregate_comments(
	aggregate="author",
	frequency="day",
	subreddit="technology",
	after="2026-04-01",
	before="2026-04-08",
)
```

## Subreddit Methods

### `search_subreddits`

```python
subs = client.search_subreddits(
	subreddit_prefix="tech",
	min_subscribers=50000,
	limit=25,
	sort="desc",
	sort_type="subscribers",
	fields=["display_name", "subscribers", "over18"],
)
```

### `get_subreddit_rules`

```python
rules = client.get_subreddit_rules(subreddits=["technology", "MachineLearning"])
```

### `get_subreddit_wikis`

```python
wikis = client.get_subreddit_wikis(
	subreddit="technology",
	paths=["index", "faq"],
	limit=10,
)
```

### `list_subreddit_wikis`

```python
wiki_paths = client.list_subreddit_wikis(subreddit="technology")
```

## User Methods

### `search_users`

```python
users = client.search_users(
	author_prefix="sam",
	min_karma=1000,
	limit=25,
	sort="desc",
	sort_type="total_karma",
)
```

### `user_user_interactions`

```python
interactions = client.user_user_interactions(
	author="spez",
	subreddit="technology",
	after="2026-04-01",
	before="2026-04-08",
	min_count=2,
	limit=100,
)
```

### `user_subreddit_interactions`

```python
subreddit_interactions = client.user_subreddit_interactions(
	author="spez",
	weight_posts=1.0,
	weight_comments=1.0,
	after="2026-04-01",
	before="2026-04-08",
	min_count=1,
	limit=100,
)
```

### `aggregate_flairs`

```python
flairs = client.aggregate_flairs(author="spez")
```

## Utility Endpoints

### `resolve_short_links`

```python
resolved = client.resolve_short_links(paths=["3g1jfiw", "3h2kxyz"])
```

### `time_series`

```python
series = client.time_series(
	key="posts",
	precision="day",
	after="2026-04-01",
	before="2026-04-08",
)
```

## Helper Functions

### `utc_now`

```python
now_utc = utc_now()
```

### `format_date`

```python
dt_str = format_date(datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc))
# "2026-04-01T12:00:00Z"
```

### `split_time_range`

```python
start = datetime(2026, 4, 1, tzinfo=timezone.utc)
end = datetime(2026, 4, 3, tzinfo=timezone.utc)
windows = split_time_range(start=start, end=end, step_hours=12)
```

### `normalize_response`

```python
items = normalize_response({"data": [{"id": "1"}, {"id": "2"}]})
```

### `deduplicate_items`

```python
deduped = deduplicate_items(
	items=[{"id": "a"}, {"id": "a"}, {"id": "b"}],
	id_field="id",
)
```

### `collect_posts_by_windows`

```python
start = datetime(2026, 4, 1, tzinfo=timezone.utc)
end = datetime(2026, 4, 8, tzinfo=timezone.utc)

posts = collect_posts_by_windows(
	client=client,
	subreddit="technology",
	start=start,
	end=end,
	step_hours=24,
	limit=100,
	fields=["id", "title", "created_utc"],
)
```

### `collect_comments_by_windows`

```python
start = datetime(2026, 4, 1, tzinfo=timezone.utc)
end = datetime(2026, 4, 8, tzinfo=timezone.utc)

comments = collect_comments_by_windows(
	client=client,
	subreddit="technology",
	start=start,
	end=end,
	step_hours=24,
	limit=100,
	fields=["id", "body", "created_utc"],
)
```

## Notes

- These examples reflect the current public API in `arcshiftwrap/arctic_shift.py`.
- Internal helper methods like `_join`, `_bool`, `_clean`, `_parse_response`, and `_rate_limit_wait` are intentionally not used directly.
