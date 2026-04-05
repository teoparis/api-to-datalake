"""
Retry decorator for network-bound operations.

Uses ``tenacity`` with exponential back-off.  Retries on:

* ``requests.exceptions.ConnectionError``
* ``requests.exceptions.Timeout``
* HTTP 429 Too Many Requests (detected via ``HTTPError`` + status code)
* HTTP 5xx Server Errors

Logs each retry attempt at WARNING level so operators can spot flaky sources.
"""

import functools
from typing import Callable, TypeVar

import requests
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from src.utils.logger import get_logger

log = get_logger(__name__)

F = TypeVar("F", bound=Callable)

# ── Maximum number of attempts (1 initial + 2 retries) ───────────────────────
_MAX_ATTEMPTS = 3
_WAIT_MIN_SECONDS = 2
_WAIT_MAX_SECONDS = 30


def _is_retryable(exc: BaseException) -> bool:
    """Return True if *exc* is a transient error worth retrying.

    Args:
        exc: The exception raised by the decorated function.

    Returns:
        ``True`` for connection errors, timeouts, and HTTP 429/5xx responses.
    """
    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        response = exc.response
        if response is not None:
            return response.status_code == 429 or response.status_code >= 500
    return False


def _log_retry(retry_state: RetryCallState) -> None:
    """Callback executed before every retry attempt."""
    exc = retry_state.outcome.exception()
    attempt = retry_state.attempt_number
    wait = retry_state.next_action.sleep  # type: ignore[union-attr]
    log.warning(
        "Retrying after transient error",
        attempt=attempt,
        next_wait_seconds=round(wait, 2),
        error=str(exc),
    )


def with_retry(func: F) -> F:
    """Decorator: retry *func* up to 3 times with exponential back-off.

    Apply to any function that performs HTTP requests or JDBC queries.

    Example::

        @with_retry
        def fetch_page(url: str) -> requests.Response:
            return requests.get(url, timeout=30)
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        decorated = retry(
            retry=retry_if_exception(_is_retryable),
            wait=wait_exponential(
                multiplier=1,
                min=_WAIT_MIN_SECONDS,
                max=_WAIT_MAX_SECONDS,
            ),
            stop=stop_after_attempt(_MAX_ATTEMPTS),
            before_sleep=_log_retry,
            reraise=True,
        )(func)
        return decorated(*args, **kwargs)

    return wrapper  # type: ignore[return-value]
