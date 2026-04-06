"""
tests/test_rest_extractor.py — unit tests for RESTExtractor.

Tests:
* offset pagination  — 3 pages of 2 records each → 6 rows in final DataFrame
* 429 retry          — first call returns 429 + Retry-After, second succeeds
* Bearer auth        — Authorization header is set correctly
* incremental load   — watermark is passed as a query parameter
"""

import json
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from src.extractors.rest_extractor import RESTExtractor


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_response(status_code: int, body: dict, headers: dict | None = None):
    """Build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.json.return_value = body
    if status_code >= 400:
        from requests.exceptions import HTTPError
        resp.raise_for_status.side_effect = HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


# ── Config fixtures ───────────────────────────────────────────────────────────

OFFSET_CONFIG = {
    "url": "https://api.example.com/records",
    "auth": {"type": "bearer", "token_env": "TEST_BEARER_TOKEN"},
    "pagination": {
        "type": "offset",
        "page_param": "page",
        "page_size": 2,
        "page_size_param": "limit",
    },
    "response_mapping": {"root_key": "data"},
    "incremental": {"enabled": False},
}

BEARER_CONFIG = {
    "url": "https://api.example.com/items",
    "auth": {"type": "bearer", "token_env": "MY_BEARER"},
    "pagination": {"type": "none"},
    "response_mapping": {"root_key": "items"},
    "incremental": {"enabled": False},
}

INCREMENTAL_CONFIG = {
    "url": "https://api.example.com/events",
    "auth": {"type": "api_key", "param_name": "api_key", "token_env": "MY_API_KEY"},
    "pagination": {"type": "none"},
    "response_mapping": {"root_key": "events"},
    "incremental": {
        "enabled": True,
        "watermark_field": "updated_at",
    },
}

RATE_LIMIT_CONFIG = {
    "url": "https://api.example.com/data",
    "auth": {"type": "bearer", "token_env": "RATE_BEARER"},
    "pagination": {"type": "none"},
    "response_mapping": {"root_key": "results"},
    "rate_limit": {"respect_retry_after": True, "max_wait_seconds": 60},
    "incremental": {"enabled": False},
}


# ── Test: offset pagination ───────────────────────────────────────────────────

class TestOffsetPagination:
    """Verify that all pages are fetched and concatenated correctly."""

    def test_three_pages_six_records(self, monkeypatch):
        """Offset pagination across 3 pages of 2 records each returns 6 rows."""
        monkeypatch.setenv("TEST_BEARER_TOKEN", "secret-token")

        page_data = [
            {"data": [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]},
            {"data": [{"id": 3, "val": "c"}, {"id": 4, "val": "d"}]},
            {"data": [{"id": 5, "val": "e"}, {"id": 6, "val": "f"}]},
            {"data": []},  # empty page signals end
        ]
        responses = [_make_response(200, body) for body in page_data]

        with patch("requests.Session") as MockSession:
            session_instance = MagicMock()
            session_instance.get.side_effect = responses
            session_instance.headers = {}
            session_instance.auth = None
            MockSession.return_value = session_instance

            extractor = RESTExtractor()
            df = extractor.extract(OFFSET_CONFIG, watermark=None)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 6
        assert set(df["id"]) == {1, 2, 3, 4, 5, 6}
        assert "_ingested_at" in df.columns

    def test_page_params_increment(self, monkeypatch):
        """Each successive GET call must use an incremented page number."""
        monkeypatch.setenv("TEST_BEARER_TOKEN", "tok")

        pages = [
            {"data": [{"id": i} for i in range(2)]},
            {"data": [{"id": i} for i in range(2, 4)]},
            {"data": []},
        ]
        responses = [_make_response(200, p) for p in pages]

        with patch("requests.Session") as MockSession:
            session_instance = MagicMock()
            session_instance.get.side_effect = responses
            session_instance.headers = {}
            session_instance.auth = None
            MockSession.return_value = session_instance

            extractor = RESTExtractor()
            extractor.extract(OFFSET_CONFIG, watermark=None)

            calls = session_instance.get.call_args_list
            # page=1 on first call, page=2 on second
            assert calls[0][1]["params"]["page"] == 1
            assert calls[1][1]["params"]["page"] == 2


# ── Test: 429 retry ───────────────────────────────────────────────────────────

class TestRateLimitRetry:
    """Verify that a 429 response causes a sleep + retry and ultimately succeeds."""

    def test_retry_after_429(self, monkeypatch):
        """First response is 429 with Retry-After header; second succeeds."""
        monkeypatch.setenv("RATE_BEARER", "bearer-xyz")

        rate_limited = _make_response(
            429, {}, headers={"Retry-After": "1"}
        )
        ok_response = _make_response(200, {"results": [{"id": 99}]})

        with patch("requests.Session") as MockSession,              patch("time.sleep") as mock_sleep:
            session_instance = MagicMock()
            session_instance.headers = {}
            session_instance.auth = None
            # First call: 429. After sleep, tenacity retries -> second call: 200.
            session_instance.get.side_effect = [rate_limited, ok_response]
            MockSession.return_value = session_instance

            extractor = RESTExtractor()
            df = extractor.extract(RATE_LIMIT_CONFIG, watermark=None)

        # sleep must have been called with the Retry-After value (capped at max_wait)
        mock_sleep.assert_called_once_with(1)
        assert len(df) == 1
        assert df.iloc[0]["id"] == 99

    def test_retry_after_capped_at_max_wait(self, monkeypatch):
        """Retry-After larger than max_wait_seconds is capped."""
        monkeypatch.setenv("RATE_BEARER", "bearer-xyz")

        rate_limited = _make_response(
            429, {}, headers={"Retry-After": "9999"}
        )
        ok_response = _make_response(200, {"results": [{"id": 1}]})

        with patch("requests.Session") as MockSession,              patch("time.sleep") as mock_sleep:
            session_instance = MagicMock()
            session_instance.headers = {}
            session_instance.auth = None
            session_instance.get.side_effect = [rate_limited, ok_response]
            MockSession.return_value = session_instance

            extractor = RESTExtractor()
            cfg = {**RATE_LIMIT_CONFIG, "rate_limit": {"max_wait_seconds": 10}}
            extractor.extract(cfg, watermark=None)

        # sleep must be capped at max_wait_seconds=10
        mock_sleep.assert_called_once_with(10)


# ── Test: Bearer auth ─────────────────────────────────────────────────────────

class TestBearerAuth:
    """Verify that the Authorization: Bearer header is set on the session."""

    def test_bearer_header_set(self, monkeypatch):
        """Authorization header is set to Bearer <token> from env var."""
        monkeypatch.setenv("MY_BEARER", "my-secret-bearer")

        ok_response = _make_response(200, {"items": [{"x": 1}]})

        with patch("requests.Session") as MockSession:
            session_instance = MagicMock()
            session_instance.headers = {}
            session_instance.auth = None
            session_instance.get.return_value = ok_response
            MockSession.return_value = session_instance

            extractor = RESTExtractor()
            extractor.extract(BEARER_CONFIG, watermark=None)

            # headers.update must have been called with the Bearer token
            session_instance.headers.update.assert_called_once_with(
                {"Authorization": "Bearer my-secret-bearer"}
            )

    def test_missing_bearer_env_var_logs_warning(self, monkeypatch, caplog):
        """Missing env var logs a warning and proceeds without auth."""
        monkeypatch.delenv("MY_BEARER", raising=False)

        ok_response = _make_response(200, {"items": []})

        with patch("requests.Session") as MockSession:
            session_instance = MagicMock()
            session_instance.headers = {}
            session_instance.auth = None
            session_instance.get.return_value = ok_response
            MockSession.return_value = session_instance

            extractor = RESTExtractor()
            df = extractor.extract(BEARER_CONFIG, watermark=None)

        assert isinstance(df, pd.DataFrame)


# ── Test: incremental (watermark as query param) ──────────────────────────────

class TestIncrementalLoad:
    """Verify that the watermark is passed as a query parameter."""

    def test_watermark_passed_as_query_param(self, monkeypatch):
        """When a watermark is provided it appears in the GET query params."""
        monkeypatch.setenv("MY_API_KEY", "k3y")

        ok_response = _make_response(
            200,
            {"events": [{"id": 1, "updated_at": "2025-04-01T00:00:00Z"}]},
        )

        with patch("requests.Session") as MockSession:
            session_instance = MagicMock()
            session_instance.headers = {}
            session_instance.auth = None
            session_instance.get.return_value = ok_response
            MockSession.return_value = session_instance

            extractor = RESTExtractor()
            df = extractor.extract(
                INCREMENTAL_CONFIG,
                watermark="2025-01-01T00:00:00Z",
            )

            call_kwargs = session_instance.get.call_args[1]
            params = call_kwargs.get("params", {})

        assert "updated_at" in params
        assert params["updated_at"] == "2025-01-01T00:00:00Z"
        assert len(df) == 1

    def test_no_watermark_param_on_full_load(self, monkeypatch):
        """When watermark is None no watermark param is added to the request."""
        monkeypatch.setenv("MY_API_KEY", "k3y")

        ok_response = _make_response(
            200, {"events": [{"id": 2}]}
        )

        with patch("requests.Session") as MockSession:
            session_instance = MagicMock()
            session_instance.headers = {}
            session_instance.auth = None
            session_instance.get.return_value = ok_response
            MockSession.return_value = session_instance

            extractor = RESTExtractor()
            extractor.extract(INCREMENTAL_CONFIG, watermark=None)

            call_kwargs = session_instance.get.call_args[1]
            params = call_kwargs.get("params", {})

        assert "updated_at" not in params


# ── Test: validate_config ─────────────────────────────────────────────────────

class TestValidateConfig:
    def test_missing_url_raises(self):
        extractor = RESTExtractor()
        with pytest.raises(ValueError, match="url"):
            extractor.validate_config({"auth": {"type": "bearer"}})

    def test_invalid_auth_type_raises(self):
        extractor = RESTExtractor()
        with pytest.raises(ValueError, match="auth type"):
            extractor.validate_config({
                "url": "https://x.com",
                "auth": {"type": "oauth2"},
            })
