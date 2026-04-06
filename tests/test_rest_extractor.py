import pytest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from src.extractors.rest_extractor import RESTExtractor


@pytest.fixture
def extractor():
    return RESTExtractor()


@pytest.fixture
def base_config():
    return {
        "url": "https://api.example.com",
        "endpoint": "/transactions",
        "auth": {"type": "bearer", "token": "test-token"},
        "pagination": {"type": "offset", "page_size": 2, "param": "page"},
        "response_key": "data",
    }


def mock_page(data, has_next=True):
    return MagicMock(
        status_code=200,
        json=lambda: {"data": data, "has_next": has_next},
        raise_for_status=lambda: None,
    )


class TestRESTExtractorPagination:
    @patch("src.extractors.rest_extractor.requests.Session")
    def test_offset_pagination_collects_all_pages(self, mock_session_cls, extractor, base_config):
        session = MagicMock()
        mock_session_cls.return_value = session
        session.get.side_effect = [
            mock_page([{"id": 1, "amount": 100}, {"id": 2, "amount": 200}], has_next=True),
            mock_page([{"id": 3, "amount": 300}], has_next=False),
        ]

        df = extractor.extract(base_config, watermark=None)

        assert len(df) == 3
        assert list(df["id"]) == [1, 2, 3]
        assert session.get.call_count == 2

    @patch("src.extractors.rest_extractor.requests.Session")
    def test_cursor_pagination(self, mock_session_cls, extractor):
        session = MagicMock()
        mock_session_cls.return_value = session

        config = {
            "url": "https://api.example.com",
            "endpoint": "/events",
            "auth": {"type": "bearer", "token": "tok"},
            "pagination": {"type": "cursor", "cursor_field": "next_cursor", "param": "cursor"},
            "response_key": "events",
        }

        page1 = MagicMock(status_code=200, raise_for_status=lambda: None)
        page1.json.return_value = {"events": [{"id": "a"}, {"id": "b"}], "next_cursor": "cur_xyz"}
        page2 = MagicMock(status_code=200, raise_for_status=lambda: None)
        page2.json.return_value = {"events": [{"id": "c"}], "next_cursor": None}

        session.get.side_effect = [page1, page2]
        df = extractor.extract(config, watermark=None)

        assert len(df) == 3
        # second call must include cursor param
        _, kwargs = session.get.call_args_list[1]
        assert kwargs["params"].get("cursor") == "cur_xyz"


class TestRESTExtractorAuth:
    @patch("src.extractors.rest_extractor.requests.Session")
    def test_bearer_auth_sets_header(self, mock_session_cls, extractor, base_config):
        session = MagicMock()
        mock_session_cls.return_value = session
        session.get.return_value = mock_page([], has_next=False)

        extractor.extract(base_config, watermark=None)

        session.headers.update.assert_called_once_with(
            {"Authorization": "Bearer test-token"}
        )


class TestRESTExtractorRetry:
    @patch("src.extractors.rest_extractor.time.sleep", return_value=None)
    @patch("src.extractors.rest_extractor.requests.Session")
    def test_retries_on_429(self, mock_session_cls, mock_sleep, extractor, base_config):
        session = MagicMock()
        mock_session_cls.return_value = session

        rate_limited = MagicMock(status_code=429)
        rate_limited.headers = {"Retry-After": "1"}
        rate_limited.raise_for_status.side_effect = Exception("429")

        ok = mock_page([{"id": 1}], has_next=False)

        session.get.side_effect = [rate_limited, ok]
        df = extractor.extract(base_config, watermark=None)

        assert len(df) == 1
        mock_sleep.assert_called()


class TestRESTExtractorIncremental:
    @patch("src.extractors.rest_extractor.requests.Session")
    def test_watermark_passed_as_query_param(self, mock_session_cls, extractor):
        session = MagicMock()
        mock_session_cls.return_value = session
        session.get.return_value = mock_page([], has_next=False)

        config = {
            "url": "https://api.example.com",
            "endpoint": "/txns",
            "auth": {"type": "api_key", "key": "mykey", "param": "apikey"},
            "pagination": {"type": "offset", "page_size": 100, "param": "page"},
            "response_key": "data",
            "watermark_field": "updated_at",
            "watermark_param": "since",
        }

        extractor.extract(config, watermark="2024-01-15T00:00:00")

        _, kwargs = session.get.call_args
        assert kwargs["params"].get("since") == "2024-01-15T00:00:00"
