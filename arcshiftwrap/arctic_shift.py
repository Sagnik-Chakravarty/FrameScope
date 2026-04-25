"""
Arctic Shift API client for arcshiftwrap.

This module wraps Arctic Shift API endpoints and adds:
- retries
- rate-limit handling
- timeout handling
- configurable sleeps
- field selection
- time-window splitting for large pulls
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Union

import requests


BASE_URL = "https://arctic-shift.photon-reddit.com"

logger = logging.getLogger(__name__)


class ArcticShiftClient:
    def __init__(
        self,
        base_url: str = BASE_URL,
        timeout: int = 90,
        sleep_seconds: float = 1.0,
        max_retries: int = 4,
        backoff_factor: float = 2.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session = requests.Session()

    @staticmethod
    def _join(values: Union[str, Iterable[str], None]) -> Optional[str]:
        if values is None:
            return None
        if isinstance(values, str):
            return values
        return ",".join(str(v) for v in values)

    @staticmethod
    def _bool(value: Optional[bool]) -> Optional[str]:
        if value is None:
            return None
        return str(value).lower()

    @staticmethod
    def _clean(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if params is None:
            return {}
        return {k: v for k, v in params.items() if v is not None}

    @staticmethod
    def _parse_response(response: requests.Response) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Could not parse JSON response. Status={response.status_code}, "
                f"Text={response.text[:500]}"
            ) from exc

    def request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Generic GET request method.

        This exists so future Arctic Shift endpoints can still be called
        without adding new wrapper methods.
        """

        url = f"{self.base_url}{endpoint}"
        clean_params = self._clean(params)

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=clean_params,
                    timeout=self.timeout,
                )

                if response.status_code == 429:
                    wait_seconds = self._rate_limit_wait(response)
                    logger.warning("Rate limited. Sleeping %s seconds.", wait_seconds)
                    time.sleep(wait_seconds)
                    continue

                # Arctic Shift sometimes returns query timeout as text/json message.
                if response.status_code == 504 or "Query timed out" in response.text:
                    if attempt >= self.max_retries:
                        raise RuntimeError(
                            f"Arctic Shift query timed out after {self.max_retries} attempts. "
                            f"Endpoint={endpoint}, params={clean_params}"
                        )

                    wait_seconds = self.backoff_factor ** attempt
                    logger.warning(
                        "Query timeout on attempt %s/%s. Sleeping %.1f seconds.",
                        attempt,
                        self.max_retries,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()

                data = self._parse_response(response)
                time.sleep(self.sleep_seconds)
                return data

            except requests.RequestException as exc:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"Request failed after {self.max_retries} attempts. "
                        f"Endpoint={endpoint}, params={clean_params}"
                    ) from exc

                wait_seconds = self.backoff_factor ** attempt
                logger.warning(
                    "Request failed on attempt %s/%s. Sleeping %.1f seconds.",
                    attempt,
                    self.max_retries,
                    wait_seconds,
                )
                time.sleep(wait_seconds)

        raise RuntimeError(f"Unexpected request failure. Endpoint={endpoint}")

    @staticmethod
    def _rate_limit_wait(response: requests.Response) -> int:
        reset = response.headers.get("X-RateLimit-Reset")

        if reset and reset.isdigit():
            reset_value = int(reset)

            # Some APIs return seconds-until-reset; others return epoch time.
            now = int(time.time())
            if reset_value > now:
                return max(reset_value - now, 1)

            return max(reset_value, 1)

        return 30

    # ---------------------------------------------------------
    # ID lookup
    # ---------------------------------------------------------

    def get_posts_by_ids(
        self,
        ids: Union[str, List[str]],
        md2html: bool = False,
        fields: Optional[Union[str, List[str]]] = None,
    ) -> Any:
        return self.request(
            "/api/posts/ids",
            {
                "ids": self._join(ids),
                "md2html": self._bool(md2html),
                "fields": self._join(fields),
            },
        )

    def get_comments_by_ids(
        self,
        ids: Union[str, List[str]],
        md2html: bool = False,
        fields: Optional[Union[str, List[str]]] = None,
    ) -> Any:
        return self.request(
            "/api/comments/ids",
            {
                "ids": self._join(ids),
                "md2html": self._bool(md2html),
                "fields": self._join(fields),
            },
        )

    def get_subreddits_by_ids(
        self,
        ids: Union[str, List[str]],
        fields: Optional[Union[str, List[str]]] = None,
    ) -> Any:
        return self.request(
            "/api/subreddits/ids",
            {
                "ids": self._join(ids),
                "fields": self._join(fields),
            },
        )

    def get_users_by_ids(
        self,
        ids: Union[str, List[str]],
        fields: Optional[Union[str, List[str]]] = None,
    ) -> Any:
        return self.request(
            "/api/users/ids",
            {
                "ids": self._join(ids),
                "fields": self._join(fields),
            },
        )

    # ---------------------------------------------------------
    # Posts and comments search
    # ---------------------------------------------------------

    def search_posts(
        self,
        subreddit: Optional[str] = None,
        author: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        limit: Union[int, str] = 100,
        sort: str = "asc",
        title: Optional[str] = None,
        selftext: Optional[str] = None,
        query: Optional[str] = None,
        link_flair_text: Optional[str] = None,
        author_flair_text: Optional[str] = None,
        url: Optional[str] = None,
        url_exact: Optional[bool] = None,
        over_18: Optional[bool] = None,
        spoiler: Optional[bool] = None,
        crosspost_parent_id: Optional[str] = None,
        md2html: bool = False,
        fields: Optional[Union[str, List[str]]] = None,
    ) -> Any:
        return self.request(
            "/api/posts/search",
            {
                "subreddit": subreddit,
                "author": author,
                "after": after,
                "before": before,
                "limit": limit,
                "sort": sort,
                "title": title,
                "selftext": selftext,
                "query": query,
                "link_flair_text": link_flair_text,
                "author_flair_text": author_flair_text,
                "url": url,
                "url_exact": self._bool(url_exact),
                "over_18": self._bool(over_18),
                "spoiler": self._bool(spoiler),
                "crosspost_parent_id": crosspost_parent_id,
                "md2html": self._bool(md2html),
                "fields": self._join(fields),
            },
        )

    def search_comments(
        self,
        subreddit: Optional[str] = None,
        author: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        limit: Union[int, str] = 100,
        sort: str = "asc",
        body: Optional[str] = None,
        link_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        author_flair_text: Optional[str] = None,
        md2html: bool = False,
        fields: Optional[Union[str, List[str]]] = None,
    ) -> Any:
        return self.request(
            "/api/comments/search",
            {
                "subreddit": subreddit,
                "author": author,
                "after": after,
                "before": before,
                "limit": limit,
                "sort": sort,
                "body": body,
                "link_id": link_id,
                "parent_id": parent_id,
                "author_flair_text": author_flair_text,
                "md2html": self._bool(md2html),
                "fields": self._join(fields),
            },
        )

    # ---------------------------------------------------------
    # Comment tree
    # ---------------------------------------------------------

    def get_comment_tree(
        self,
        link_id: str,
        parent_id: Optional[str] = None,
        limit: int = 9999,
        start_breadth: int = 4,
        start_depth: int = 4,
        md2html: bool = False,
    ) -> Any:
        return self.request(
            "/api/comments/tree",
            {
                "link_id": link_id,
                "parent_id": parent_id,
                "limit": limit,
                "start_breadth": start_breadth,
                "start_depth": start_depth,
                "md2html": self._bool(md2html),
            },
        )

    # ---------------------------------------------------------
    # Aggregations
    # ---------------------------------------------------------

    def aggregate_posts(
        self,
        aggregate: str,
        frequency: Optional[str] = None,
        subreddit: Optional[str] = None,
        author: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
        min_count: Optional[int] = None,
        sort: Optional[str] = None,
        **extra_filters: Any,
    ) -> Any:
        params = {
            "aggregate": aggregate,
            "frequency": frequency,
            "subreddit": subreddit,
            "author": author,
            "after": after,
            "before": before,
            "limit": limit,
            "min_count": min_count,
            "sort": sort,
            **extra_filters,
        }
        return self.request("/api/posts/search/aggregate", params)

    def aggregate_comments(
        self,
        aggregate: str,
        frequency: Optional[str] = None,
        subreddit: Optional[str] = None,
        author: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        limit: Optional[int] = None,
        min_count: Optional[int] = None,
        sort: Optional[str] = None,
        **extra_filters: Any,
    ) -> Any:
        params = {
            "aggregate": aggregate,
            "frequency": frequency,
            "subreddit": subreddit,
            "author": author,
            "after": after,
            "before": before,
            "limit": limit,
            "min_count": min_count,
            "sort": sort,
            **extra_filters,
        }
        return self.request("/api/comments/search/aggregate", params)

    # ---------------------------------------------------------
    # Subreddits
    # ---------------------------------------------------------

    def search_subreddits(
        self,
        subreddit: Optional[str] = None,
        subreddit_prefix: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        min_subscribers: Optional[int] = None,
        max_subscribers: Optional[int] = None,
        over18: Optional[bool] = None,
        limit: int = 25,
        sort: str = "desc",
        sort_type: str = "subscribers",
        fields: Optional[Union[str, List[str]]] = None,
    ) -> Any:
        return self.request(
            "/api/subreddits/search",
            {
                "subreddit": subreddit,
                "subreddit_prefix": subreddit_prefix,
                "after": after,
                "before": before,
                "min_subscribers": min_subscribers,
                "max_subscribers": max_subscribers,
                "over18": self._bool(over18),
                "limit": limit,
                "sort": sort,
                "sort_type": sort_type,
                "fields": self._join(fields),
            },
        )

    def get_subreddit_rules(self, subreddits: Union[str, List[str]]) -> Any:
        return self.request(
            "/api/subreddits/rules",
            {"subreddits": self._join(subreddits)},
        )

    def get_subreddit_wikis(
        self,
        subreddit: Optional[str] = None,
        paths: Optional[Union[str, List[str]]] = None,
        limit: int = 100,
    ) -> Any:
        return self.request(
            "/api/subreddits/wikis",
            {
                "subreddit": subreddit,
                "paths": self._join(paths),
                "limit": limit,
            },
        )

    def list_subreddit_wikis(self, subreddit: str) -> Any:
        return self.request(
            "/api/subreddits/wikis/list",
            {"subreddit": subreddit},
        )

    # ---------------------------------------------------------
    # Users
    # ---------------------------------------------------------

    def search_users(
        self,
        author: Optional[str] = None,
        author_prefix: Optional[str] = None,
        min_num_posts: Optional[int] = None,
        min_num_comments: Optional[int] = None,
        active_since: Optional[str] = None,
        min_karma: Optional[int] = None,
        limit: int = 25,
        sort: str = "desc",
        sort_type: str = "total_karma",
    ) -> Any:
        return self.request(
            "/api/users/search",
            {
                "author": author,
                "author_prefix": author_prefix,
                "min_num_posts": min_num_posts,
                "min_num_comments": min_num_comments,
                "active_since": active_since,
                "min_karma": min_karma,
                "limit": limit,
                "sort": sort,
                "sort_type": sort_type,
            },
        )

    def user_user_interactions(
        self,
        author: str,
        subreddit: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        min_count: Optional[int] = None,
        limit: Optional[int] = 100,
        list_mode: bool = False,
    ) -> Any:
        endpoint = (
            "/api/users/interactions/users/list"
            if list_mode
            else "/api/users/interactions/users"
        )

        return self.request(
            endpoint,
            {
                "author": author,
                "subreddit": subreddit,
                "after": after,
                "before": before,
                "min_count": min_count,
                "limit": limit,
            },
        )

    def user_subreddit_interactions(
        self,
        author: str,
        weight_posts: float = 1.0,
        weight_comments: float = 1.0,
        after: Optional[str] = None,
        before: Optional[str] = None,
        min_count: Optional[int] = None,
        limit: Optional[int] = 100,
    ) -> Any:
        return self.request(
            "/api/users/interactions/subreddits",
            {
                "author": author,
                "weight_posts": weight_posts,
                "weight_comments": weight_comments,
                "after": after,
                "before": before,
                "min_count": min_count,
                "limit": limit,
            },
        )

    def aggregate_flairs(self, author: str) -> Any:
        return self.request(
            "/api/users/aggregate_flairs",
            {"author": author},
        )

    # ---------------------------------------------------------
    # Short links
    # ---------------------------------------------------------

    def resolve_short_links(self, paths: Union[str, List[str]]) -> Any:
        return self.request(
            "/api/short_links",
            {"paths": self._join(paths)},
        )

    # ---------------------------------------------------------
    # Time series
    # ---------------------------------------------------------

    def time_series(
        self,
        key: str,
        precision: str,
        after: Optional[str] = None,
        before: Optional[str] = None,
    ) -> Any:
        return self.request(
            "/api/time_series",
            {
                "key": key,
                "precision": precision,
                "after": after,
                "before": before,
            },
        )


# ---------------------------------------------------------
# Helper functions for robust collection
# ---------------------------------------------------------


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_date(dt: datetime) -> str:
    """
    Arctic Shift accepts ISO-like dates. Keep this compact and readable.
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def split_time_range(
    start: datetime,
    end: datetime,
    step_hours: int = 24,
) -> List[tuple[datetime, datetime]]:
    """
    Split a large collection window into smaller windows.

    This is important because Arctic Shift does not provide normal pagination.
    For active subreddits, smaller windows reduce timeout/truncation risk.
    """
    if start >= end:
        raise ValueError("start must be earlier than end")

    windows = []
    current = start

    while current < end:
        next_time = min(current + timedelta(hours=step_hours), end)
        windows.append((current, next_time))
        current = next_time

    return windows


def normalize_response(data: Any) -> List[Dict[str, Any]]:
    """
    Arctic Shift endpoints usually return lists, but this keeps the pipeline
    safe if an endpoint returns a dict with data/results/items.
    """
    if data is None:
        return []

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ["data", "results", "items"]:
            if key in data and isinstance(data[key], list):
                return data[key]

        return [data]

    return []


def deduplicate_items(
    items: List[Dict[str, Any]],
    id_field: str = "id",
) -> List[Dict[str, Any]]:
    seen = set()
    deduped = []

    for item in items:
        item_id = item.get(id_field)

        if item_id is None:
            deduped.append(item)
            continue

        if item_id not in seen:
            seen.add(item_id)
            deduped.append(item)

    return deduped


def collect_posts_by_windows(
    client: ArcticShiftClient,
    subreddit: str,
    start: datetime,
    end: datetime,
    step_hours: int = 24,
    limit: int = 100,
    fields: Optional[Union[str, List[str]]] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    all_posts: List[Dict[str, Any]] = []

    for window_start, window_end in split_time_range(start, end, step_hours):
        logger.info(
            "Fetching posts: r/%s from %s to %s",
            subreddit,
            format_date(window_start),
            format_date(window_end),
        )

        data = client.search_posts(
            subreddit=subreddit,
            after=format_date(window_start),
            before=format_date(window_end),
            limit=limit,
            sort="asc",
            fields=fields,
            **kwargs,
        )

        posts = normalize_response(data)
        all_posts.extend(posts)

    return deduplicate_items(all_posts)


def collect_comments_by_windows(
    client: ArcticShiftClient,
    subreddit: str,
    start: datetime,
    end: datetime,
    step_hours: int = 24,
    limit: int = 100,
    fields: Optional[Union[str, List[str]]] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    all_comments: List[Dict[str, Any]] = []

    for window_start, window_end in split_time_range(start, end, step_hours):
        logger.info(
            "Fetching comments: r/%s from %s to %s",
            subreddit,
            format_date(window_start),
            format_date(window_end),
        )

        data = client.search_comments(
            subreddit=subreddit,
            after=format_date(window_start),
            before=format_date(window_end),
            limit=limit,
            sort="asc",
            fields=fields,
            **kwargs,
        )

        comments = normalize_response(data)
        all_comments.extend(comments)

    return deduplicate_items(all_comments)