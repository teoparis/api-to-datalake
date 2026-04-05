"""
WatermarkStore — persist and retrieve the last-seen watermark for each source.

Watermarks are stored as a JSON file.  In local development the file lives on
disk; in production it is uploaded to / downloaded from ADLS Gen2 so that all
pipeline workers share the same state.

The file is protected with an ``fcntl`` advisory lock on POSIX systems to
prevent concurrent writes when multiple sources run in threads.
"""

import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

from src.utils.logger import get_logger

log = get_logger(__name__)

_DEFAULT_STORE_PATH = Path(os.getenv("WATERMARK_STORE_PATH", ".watermarks"))
_WATERMARK_FILE = "watermarks.json"


class WatermarkStore:
    """Persist the last-processed watermark value for each ingestion source.

    Args:
        store_path: Directory where ``watermarks.json`` is written.
                    Defaults to the ``WATERMARK_STORE_PATH`` env var or
                    ``.watermarks/`` in the working directory.
    """

    def __init__(self, store_path: Optional[Union[str, Path]] = None) -> None:
        self._store_path = Path(store_path or _DEFAULT_STORE_PATH)
        self._store_path.mkdir(parents=True, exist_ok=True)
        self._file = self._store_path / _WATERMARK_FILE
        log.debug("WatermarkStore initialised", path=str(self._file))

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _read(self) -> dict:
        """Read and return the full watermark dict from disk.

        Returns an empty dict if the file does not exist yet.
        """
        if not self._file.exists():
            return {}
        with open(self._file, "r", encoding="utf-8") as fh:
            try:
                return json.load(fh)
            except json.JSONDecodeError:
                log.warning("Watermark file corrupted — resetting", path=str(self._file))
                return {}

    def _write(self, data: dict) -> None:
        """Atomically write *data* to the watermark file with an fcntl lock."""
        with open(self._file, "a+", encoding="utf-8") as fh:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX)
                fh.seek(0)
                fh.truncate()
                json.dump(data, fh, indent=2, default=str)
                fh.flush()
                os.fsync(fh.fileno())
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)

    # ── Public API ────────────────────────────────────────────────────────────

    def get_last_watermark(self, source_name: str) -> Optional[str]:
        """Return the stored watermark for *source_name*, or ``None``.

        Args:
            source_name: Unique source identifier (matches key in sources.yaml).

        Returns:
            The watermark as a string (ISO 8601 datetime or epoch integer),
            or ``None`` if no watermark has been recorded yet.
        """
        data = self._read()
        value = data.get(source_name)
        log.debug(
            "get_last_watermark",
            source=source_name,
            watermark=value,
        )
        return value

    def update_watermark(self, source_name: str, value: Any) -> None:
        """Persist *value* as the new watermark for *source_name*.

        Args:
            source_name: Unique source identifier.
            value: New watermark value.  Will be coerced to string if it is a
                   ``datetime`` object.
        """
        if isinstance(value, datetime):
            # Ensure we always store UTC ISO 8601
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            value = value.isoformat()

        data = self._read()
        old = data.get(source_name)
        data[source_name] = str(value)
        self._write(data)
        log.info(
            "Watermark updated",
            source=source_name,
            old=old,
            new=str(value),
        )

    def reset_watermark(self, source_name: str) -> None:
        """Delete the stored watermark for *source_name* (forces full reload).

        Args:
            source_name: Unique source identifier.
        """
        data = self._read()
        removed = data.pop(source_name, None)
        self._write(data)
        if removed is not None:
            log.info("Watermark reset", source=source_name, previous=removed)
        else:
            log.warning("No watermark found to reset", source=source_name)

    def all_watermarks(self) -> dict:
        """Return a copy of all stored watermarks.

        Returns:
            Dict mapping source names to their last watermark string.
        """
        return dict(self._read())
