"""
Structured logging with loguru.

Every log record automatically includes the active ``correlation_id`` so that
all entries for a single pipeline run can be correlated in a log aggregation
system (e.g. Azure Monitor, Datadog).

Usage::

    from src.utils.logger import get_logger, set_correlation_id

    set_correlation_id("run-abc-123")
    log = get_logger(__name__)
    log.info("Starting extraction", source="exchange_rates")
"""

import sys
import uuid
from contextvars import ContextVar
from typing import Optional

from loguru import logger

# ── Context variable: one value per async task / thread ──────────────────────
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def set_correlation_id(run_id: Optional[str] = None) -> str:
    """Set the correlation ID for the current execution context.

    Args:
        run_id: Explicit ID to use. If ``None`` a UUID4 is generated.

    Returns:
        The ID that was set.
    """
    cid = run_id or str(uuid.uuid4())
    _correlation_id.set(cid)
    return cid


def get_correlation_id() -> str:
    """Return the active correlation ID, or an empty string if not set."""
    return _correlation_id.get()


def _correlation_patcher(record: dict) -> None:
    """Inject correlation_id into every log record."""
    record["extra"].setdefault("correlation_id", get_correlation_id())


# ── Sink format ───────────────────────────────────────────────────────────────
_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<yellow>cid={extra[correlation_id]}</yellow> | "
    "{message}"
)


def _configure_logger(level: str = "INFO") -> None:
    """Remove default sink and add a structured console sink."""
    logger.remove()
    logger.add(
        sys.stderr,
        format=_LOG_FORMAT,
        level=level,
        colorize=True,
        backtrace=True,
        diagnose=True,
    )
    # Optional file sink — activate by setting LOG_FILE env var
    import os
    log_file = os.getenv("LOG_FILE")
    if log_file:
        logger.add(
            log_file,
            format=_LOG_FORMAT,
            level=level,
            rotation="100 MB",
            retention=10,
            compression="gz",
            serialize=False,
        )

    logger.configure(patcher=_correlation_patcher)


# Initialise once at import time; callers can call _configure_logger again
# to change the level (cli.py does this for --verbose).
import os as _os
_configure_logger(level=_os.getenv("LOG_LEVEL", "INFO"))


def get_logger(name: str):
    """Return a loguru logger bound to *name*.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A loguru ``Logger`` instance with the module name bound as extra context.
    """
    return logger.bind(name=name)
