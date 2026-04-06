"""
RESTExtractor — pull data from HTTP/REST APIs.

Supports:
* Auth: api_key (query param), Bearer token (header), HTTP Basic auth
* Pagination: offset/page-based, cursor-based, next_url (Link header / body field)
* Incremental loads: appends a watermark filter as a query parameter
* 429 rate-limit handling: reads ``Retry-After`` header, sleeps, retries
* Automatic ``_ingested_at`` column on every returned DataFrame
"""

import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
import requests

from src.extractors.base_extractor import BaseExtractor
from src.utils.logger import get_logger
from src.utils.retry import with_retry

log = get_logger(__name__)

_DEFAULT_PAGE_SIZE = 100
_DEFAULT_MAX_PAGES = 500  # safety cap to avoid infinite loops
_DEFAULT_TIMEOUT = 30     # seconds


def _get_nested(obj: dict, dotted_key: str) -> Any:
    """Retrieve a value from a nested dict using a dotted key path.

    Example::

        _get_nested({"meta": {"next_cursor": "abc"}}, "meta.next_cursor")
        # → "abc"
    """
    parts = dotted_key.split(".")
    for part in parts:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


class RESTExtractor(BaseExtractor):
    """Extract records from a REST API and return them as a DataFrame.

    Configuration reference (``config/sources.yaml`` block)::

        type: rest
        url: "https://api.example.com/v1/records"
        auth:
          type: api_key          # api_key | bearer | basic
          param_name: app_id     # for api_key: query-param name
          token_env: MY_API_KEY  # env var holding the secret
          # for basic:  user_env + password_env
        pagination:
          type: offset           # none | offset | cursor | next_url
          page_param: page       # query param for page number (offset mode)
          page_size: 100
          page_size_param: limit
          cursor_response_field: meta.next_cursor  # dotted path (cursor mode)
          cursor_request_param: cursor
        incremental:
          enabled: true
          watermark_field: updated_at  # query param name used for filtering
        response_mapping:
          root_key: data         # JSON key holding the list of records
        rate_limit:
          respect_retry_after: true
          max_wait_seconds: 60
    """

    # ── Validation ────────────────────────────────────────────────────────────

    def validate_config(self, config: dict) -> None:
        """Ensure the minimum required keys are present in *config*.

        Args:
            config: Raw source configuration dict.

        Raises:
            ValueError: If ``url`` or ``auth`` are missing.
        """
        if "url" not in config:
            raise ValueError("REST source config must include 'url'")
        auth = config.get("auth", {})
        auth_type = auth.get("type", "api_key")
        if auth_type not in ("api_key", "bearer", "basic"):
            raise ValueError(
                f"Unknown auth type '{auth_type}'. Use: api_key, bearer, basic"
            )
        log.debug("RESTExtractor config valid", url=config["url"])

    # ── Auth helpers ──────────────────────────────────────────────────────────

    def _build_session(self, auth_cfg: dict) -> tuple[requests.Session, dict]:
        """Create a requests Session configured with the appropriate auth.

        Returns:
            Tuple of (session, extra_params) where extra_params are query
            parameters that must be merged into every request (api_key mode).
        """
        session = requests.Session()
        extra_params: dict = {}
        auth_type = auth_cfg.get("type", "api_key")

        if auth_type == "api_key":
            token_env = auth_cfg.get("token_env", "")
            token = os.environ.get(token_env, "")
            param_name = auth_cfg.get("param_name", "api_key")
            if token:
                extra_params[param_name] = token
            else:
                log.warning("API key env var not set", env=token_env)

        elif auth_type == "bearer":
            token_env = auth_cfg.get("token_env", "")
            token = os.environ.get(token_env, "")
            if token:
                session.headers.update({"Authorization": f"Bearer {token}"})
            else:
                log.warning("Bearer token env var not set", env=token_env)

        elif auth_type == "basic":
            user_env = auth_cfg.get("user_env", "")
            pass_env = auth_cfg.get("password_env", "")
            user = os.environ.get(user_env, "")
            password = os.environ.get(pass_env, "")
            if user and password:
                session.auth = (user, password)
            else:
                log.warning(
                    "Basic auth credentials not fully set",
                    user_env=user_env,
                    pass_env=pass_env,
                )

        return session, extra_params

    # ── Single-page fetch with 429 handling ───────────────────────────────────

    def _fetch_page(
        self,
        session: requests.Session,
        url: str,
        params: dict,
        max_wait: int = 60,
    ) -> dict:
        """Fetch a single page, retrying once on HTTP 429 Retry-After.

        The :func:`~src.utils.retry.with_retry` decorator handles transient
        ConnectionError / Timeout / 5xx automatically.  This method adds an
        explicit sleep on 429 so we honour the server's requested back-off
        before the next tenacity attempt fires.

        Args:
            session: Authenticated requests Session.
            url: Endpoint URL.
            params: Query parameters for this request.
            max_wait: Maximum seconds to sleep on 429 Retry-After.

        Returns:
            Parsed JSON response body as a dict.

        Raises:
            requests.HTTPError: On non-retryable HTTP errors.
        """
        @with_retry
        def _do_get() -> requests.Response:
            resp = session.get(url, params=params, timeout=_DEFAULT_TIMEOUT)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "5"))
                wait = min(retry_after, max_wait)
                log.warning(
                    "Rate limited (429) — sleeping",
                    retry_after=retry_after,
                    sleeping=wait,
                )
                time.sleep(wait)
                resp.raise_for_status()   # will trigger tenacity retry
            resp.raise_for_status()
            return resp

        response = _do_get()
        return response.json()

    # ── Pagination strategies ─────────────────────────────────────────────────

    def _paginate_offset(
        self,
        session: requests.Session,
        url: str,
        base_params: dict,
        pagination_cfg: dict,
        max_wait: int,
    ) -> list[dict]:
        """Collect all records using offset/page-number pagination.

        Args:
            session: Authenticated requests Session.
            url: Endpoint URL.
            base_params: Static query params (auth, watermark, page size).
            pagination_cfg: ``pagination`` block from source config.
            max_wait: Max seconds to wait on rate-limit response.

        Returns:
            Flat list of all record dicts across all pages.
        """
        page_param = pagination_cfg.get("page_param", "page")
        offset_param = pagination_cfg.get("offset_param", None)
        page_size = pagination_cfg.get("page_size", _DEFAULT_PAGE_SIZE)
        page_size_param = pagination_cfg.get("page_size_param", "limit")
        root_key = None  # resolved per call from response_mapping

        records: list[dict] = []
        page = 1
        offset = 0

        for _ in range(_DEFAULT_MAX_PAGES):
            params = {**base_params, page_size_param: page_size}
            if offset_param:
                params[offset_param] = offset
            else:
                params[page_param] = page

            body = self._fetch_page(session, url, params, max_wait)
            page_records = self._extract_records(body, root_key)

            if not page_records:
                log.debug("No more records — stopping pagination", page=page)
                break

            records.extend(page_records)
            log.debug("Fetched page", page=page, count=len(page_records))

            if len(page_records) < page_size:
                # Last page — fewer records than page size
                break

            page += 1
            offset += page_size

        return records

    def _paginate_cursor(
        self,
        session: requests.Session,
        url: str,
        base_params: dict,
        pagination_cfg: dict,
        max_wait: int,
    ) -> list[dict]:
        """Collect records using cursor-based pagination.

        Args:
            session: Authenticated requests Session.
            url: Endpoint URL.
            base_params: Static query params.
            pagination_cfg: ``pagination`` config block.
            max_wait: Max rate-limit wait seconds.

        Returns:
            All records across all cursor pages.
        """
        cursor_field = pagination_cfg.get("cursor_response_field", "next_cursor")
        cursor_param = pagination_cfg.get("cursor_request_param", "cursor")
        page_size = pagination_cfg.get("page_size", _DEFAULT_PAGE_SIZE)
        page_size_param = pagination_cfg.get("page_size_param", "limit")

        records: list[dict] = []
        cursor: Optional[str] = None
        page = 0

        for _ in range(_DEFAULT_MAX_PAGES):
            params = {**base_params, page_size_param: page_size}
            if cursor:
                params[cursor_param] = cursor

            body = self._fetch_page(session, url, params, max_wait)
            page_records = self._extract_records(body, None)

            records.extend(page_records)
            page += 1
            log.debug("Cursor page fetched", page=page, count=len(page_records))

            cursor = _get_nested(body, cursor_field)
            if not cursor:
                log.debug("No next cursor — pagination complete")
                break

        return records

    def _paginate_next_url(
        self,
        session: requests.Session,
        url: str,
        base_params: dict,
        pagination_cfg: dict,
        max_wait: int,
    ) -> list[dict]:
        """Collect records by following ``next`` URL fields in the response body.

        Args:
            session: Authenticated requests Session.
            url: Starting endpoint URL.
            base_params: Initial query params (applied to first request only).
            pagination_cfg: ``pagination`` config block.
            max_wait: Max rate-limit wait seconds.

        Returns:
            All records across all pages.
        """
        next_field = pagination_cfg.get("next_url_field", "next")
        records: list[dict] = []
        next_url: Optional[str] = url
        first = True
        page = 0

        for _ in range(_DEFAULT_MAX_PAGES):
            if not next_url:
                break
            params = base_params if first else {}
            first = False
            body = self._fetch_page(session, next_url, params, max_wait)
            records.extend(self._extract_records(body, None))
            page += 1
            log.debug("next_url page fetched", page=page, next=next_url)
            next_url = _get_nested(body, next_field)

        return records

    # ── Record extraction helper ──────────────────────────────────────────────

    def _extract_records(self, body: Any, root_key: Optional[str]) -> list[dict]:
        """Pull the list of records out of the API response body.

        Args:
            body: Parsed JSON response (dict or list).
            root_key: Dotted key path to the records array, e.g. ``"data"``
                      or ``"results.items"``.  If ``None`` and *body* is a
                      list, it is returned as-is.

        Returns:
            List of record dicts.
        """
        if isinstance(body, list):
            return body
        if root_key:
            value = _get_nested(body, root_key)
            if isinstance(value, list):
                return value
            # Sometimes the root_key points to a dict of {key: value} pairs
            # (e.g. exchange rates: {"USD": 1.0, "EUR": 0.91, ...})
            if isinstance(value, dict):
                return [{"_key": k, "_value": v} for k, v in value.items()]
        # Fallback: try common envelope keys
        for key in ("data", "results", "items", "records"):
            if key in body and isinstance(body[key], list):
                return body[key]
        # Last resort: if body is a single dict, wrap it
        if isinstance(body, dict):
            return [body]
        return []

    # ── Main extract ──────────────────────────────────────────────────────────

    def extract(
        self,
        config: dict,
        watermark: Optional[str] = None,
    ) -> pd.DataFrame:
        """Extract records from a REST API and return a DataFrame.

        Args:
            config: Source configuration dict (``sources.yaml`` block).
            watermark: Last-seen watermark value, or ``None`` for full load.

        Returns:
            DataFrame with one row per API record plus ``_ingested_at`` column.
        """
        self.validate_config(config)

        url = config["url"]
        auth_cfg = config.get("auth", {})
        pagination_cfg = config.get("pagination", {"type": "none"})
        incremental_cfg = config.get("incremental", {})
        response_mapping = config.get("response_mapping", {})
        rate_limit_cfg = config.get("rate_limit", {})
        max_wait = rate_limit_cfg.get("max_wait_seconds", 60)
        root_key = response_mapping.get("root_key")

        session, extra_params = self._build_session(auth_cfg)
        base_params = dict(extra_params)

        # Add watermark filter if incremental is enabled
        if incremental_cfg.get("enabled") and watermark:
            wm_field = incremental_cfg.get("watermark_field", "updated_at")
            base_params[wm_field] = watermark
            log.info("Incremental load", watermark_field=wm_field, since=watermark)
        else:
            log.info("Full load", url=url)

        # Resolve root_key for record extraction
        pagination_type = pagination_cfg.get("type", "none")

        if pagination_type == "none":
            body = self._fetch_page(session, url, base_params, max_wait)
            records = self._extract_records(body, root_key)

        elif pagination_type == "offset":
            records = self._paginate_offset(
                session, url, base_params, pagination_cfg, max_wait
            )

        elif pagination_type == "cursor":
            records = self._paginate_cursor(
                session, url, base_params, pagination_cfg, max_wait
            )

        elif pagination_type == "next_url":
            records = self._paginate_next_url(
                session, url, base_params, pagination_cfg, max_wait
            )

        else:
            raise ValueError(f"Unknown pagination type: '{pagination_type}'")

        log.info("Extraction complete", url=url, total_records=len(records))

        if not records:
            return pd.DataFrame()

        df = pd.json_normalize(records)
        df["_ingested_at"] = datetime.now(timezone.utc)
        return df
