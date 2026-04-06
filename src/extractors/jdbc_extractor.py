"""
JDBCExtractor — pull data from relational databases via pyodbc.

Features:
* Connection string built from config (driver, host, port, database, user, password)
* Incremental load: ``WHERE {watermark_col} > ?`` with ORDER BY
* Chunked reads via ``cursor.fetchmany(batch_size)`` to avoid loading
  massive result sets into memory all at once
* Type coercion: Decimal → float, date/datetime → pandas UTC datetime
* ``_ingested_at`` column on every returned DataFrame
* Context manager for safe connection lifecycle management
"""

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Generator, Iterator, Optional

import pandas as pd
import pyodbc

from src.extractors.base_extractor import BaseExtractor
from src.utils.logger import get_logger
from src.utils.retry import with_retry

log = get_logger(__name__)

_DEFAULT_BATCH_SIZE = 5000


def _build_connection_string(conn_cfg: dict, options: dict) -> str:
    """Assemble a pyodbc connection string from config and env vars.

    Args:
        conn_cfg: ``connection`` dict from the source config block.
        options: Extra driver key-value options (e.g. Encrypt, Timeout).

    Returns:
        A semicolon-delimited ODBC connection string.

    Raises:
        EnvironmentError: If a required env var is not set.
    """
    def _env(key: str) -> str:
        env_var = conn_cfg[key]
        value = os.environ.get(env_var)
        if not value:
            raise EnvironmentError(
                f"Required env var '{env_var}' (for JDBC '{key[:-4]}') is not set"
            )
        return value

    driver = conn_cfg.get("driver", "{ODBC Driver 18 for SQL Server}")
    host = _env("host_env")
    port = os.environ.get(conn_cfg.get("port_env", ""), "1433")
    database = _env("database_env")
    user = _env("user_env")
    password = _env("password_env")

    parts = [
        f"DRIVER={driver}",
        f"SERVER={host},{port}",
        f"DATABASE={database}",
        f"UID={user}",
        f"PWD={password}",
    ]
    for k, v in options.items():
        parts.append(f"{k}={v}")

    return ";".join(parts)


def _coerce_value(value):
    """Coerce a single field value to a Python type that pandas handles well.

    * ``Decimal`` → ``float``
    * ``datetime.date`` (non-datetime) → ``datetime`` at midnight UTC
    * ``datetime.datetime`` → UTC-aware ``datetime``
    * Everything else is returned as-is.

    Args:
        value: Raw value from the ODBC cursor.

    Returns:
        Coerced value.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    # date but not datetime
    import datetime as dt_mod
    if isinstance(value, dt_mod.date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    return value


class JDBCExtractor(BaseExtractor):
    """Extract data from a SQL database via pyodbc.

    Configuration reference (``config/sources.yaml`` block)::

        type: jdbc
        connection:
          driver: "{ODBC Driver 18 for SQL Server}"
          host_env: JDBC_HOST
          port_env: JDBC_PORT
          database_env: JDBC_DATABASE
          user_env: JDBC_USER
          password_env: JDBC_PASSWORD
          options:
            Encrypt: "yes"
            TrustServerCertificate: "no"
        query:
          table: dbo.transactions
          columns: [id, amount, created_at]
        incremental:
          enabled: true
          watermark_column: created_at
          watermark_type: datetime
          default_watermark: "2020-01-01T00:00:00"
        batch_size: 5000
    """

    # ── Validation ────────────────────────────────────────────────────────────

    def validate_config(self, config: dict) -> None:
        """Check that the JDBC config contains the required keys.

        Args:
            config: Raw source configuration dict.

        Raises:
            ValueError: If ``connection`` or ``query.table`` are missing.
        """
        if "connection" not in config:
            raise ValueError("JDBC source config must include 'connection' block")
        conn = config["connection"]
        for key in ("host_env", "database_env", "user_env", "password_env"):
            if key not in conn:
                raise ValueError(f"JDBC connection config missing '{key}'")
        query_cfg = config.get("query", {})
        if "table" not in query_cfg:
            raise ValueError("JDBC source config must include 'query.table'")
        log.debug("JDBCExtractor config valid", table=query_cfg["table"])

    # ── Connection context manager ────────────────────────────────────────────

    @contextmanager
    def _connect(self, config: dict) -> Generator[pyodbc.Connection, None, None]:
        """Yield a live pyodbc Connection and ensure it is closed on exit.

        Args:
            config: Source configuration dict.

        Yields:
            An open :class:`pyodbc.Connection`.
        """
        conn_cfg = config["connection"]
        options = conn_cfg.get("options", {})
        conn_str = _build_connection_string(conn_cfg, options)
        log.debug("Opening JDBC connection", driver=conn_cfg.get("driver"))
        conn = pyodbc.connect(conn_str)
        try:
            yield conn
        finally:
            conn.close()
            log.debug("JDBC connection closed")

    # ── SQL builder ───────────────────────────────────────────────────────────

    def _build_query(self, config: dict, watermark: Optional[str]) -> tuple[str, list]:
        """Build the SELECT statement and its parameters.

        For incremental loads the query is::

            SELECT <cols> FROM <table>
            WHERE <watermark_col> > ?
            ORDER BY <watermark_col>

        For full loads the WHERE clause is omitted.

        Args:
            config: Source configuration dict.
            watermark: Last watermark value, or ``None`` for full load.

        Returns:
            Tuple of (sql_string, params_list).
        """
        query_cfg = config.get("query", {})
        table = query_cfg["table"]
        columns = query_cfg.get("columns")
        col_clause = ", ".join(columns) if columns else "*"

        incremental_cfg = config.get("incremental", {})
        wm_col = incremental_cfg.get("watermark_column", "updated_at")
        wm_type = incremental_cfg.get("watermark_type", "datetime")
        default_wm = incremental_cfg.get("default_watermark")

        params: list = []

        effective_watermark = watermark or default_wm
        if incremental_cfg.get("enabled") and effective_watermark:
            if wm_type in ("datetime", "iso8601"):
                # Parse ISO string to datetime for ODBC parameter binding
                try:
                    wm_value = datetime.fromisoformat(
                        effective_watermark.replace("Z", "+00:00")
                    )
                except ValueError:
                    wm_value = effective_watermark
            elif wm_type == "integer":
                wm_value = int(effective_watermark)
            else:
                wm_value = effective_watermark

            sql = (
                f"SELECT {col_clause} FROM {table} "
                f"WHERE {wm_col} > ? "
                f"ORDER BY {wm_col}"
            )
            params = [wm_value]
            log.info(
                "Incremental query",
                table=table,
                watermark_col=wm_col,
                since=effective_watermark,
            )
        else:
            sql = f"SELECT {col_clause} FROM {table}"
            log.info("Full load query", table=table)

        return sql, params

    # ── Chunked fetch ─────────────────────────────────────────────────────────

    def _iter_chunks(
        self,
        cursor: pyodbc.Cursor,
        batch_size: int,
    ) -> Iterator[list[list]]:
        """Yield successive chunks of rows from the cursor.

        Args:
            cursor: Executed pyodbc cursor.
            batch_size: Number of rows per chunk.

        Yields:
            List of rows (each row is a list of values).
        """
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [list(row) for row in rows]

    # ── Main extract ──────────────────────────────────────────────────────────

    def extract(
        self,
        config: dict,
        watermark: Optional[str] = None,
    ) -> pd.DataFrame:
        """Extract rows from the configured SQL table and return a DataFrame.

        Args:
            config: Source configuration dict.
            watermark: Last-seen watermark value, or ``None`` for full load.

        Returns:
            DataFrame with one row per DB record plus ``_ingested_at`` column.
        """
        self.validate_config(config)
        batch_size = config.get("batch_size", _DEFAULT_BATCH_SIZE)
        sql, params = self._build_query(config, watermark)

        all_rows: list[list] = []
        column_names: list[str] = []

        with self._connect(config) as conn:
            cursor = conn.cursor()

            @with_retry
            def _execute():
                cursor.execute(sql, params)

            _execute()
            column_names = [col[0] for col in cursor.description]
            log.debug("Query executed", columns=column_names)

            chunk_num = 0
            for chunk in self._iter_chunks(cursor, batch_size):
                # Coerce types per row
                coerced = [
                    [_coerce_value(v) for v in row]
                    for row in chunk
                ]
                all_rows.extend(coerced)
                chunk_num += 1
                log.debug(
                    "Chunk fetched",
                    chunk=chunk_num,
                    rows_in_chunk=len(chunk),
                    total_so_far=len(all_rows),
                )

        log.info(
            "JDBC extraction complete",
            table=config["query"]["table"],
            total_rows=len(all_rows),
        )

        if not all_rows:
            # Return empty DataFrame with correct columns + _ingested_at
            df = pd.DataFrame(columns=column_names + ["_ingested_at"])
            return df

        df = pd.DataFrame(all_rows, columns=column_names)

        # Convert any remaining datetime columns to pandas UTC datetime
        for col in df.select_dtypes(include=["object"]).columns:
            pass  # string columns left as-is

        df["_ingested_at"] = datetime.now(timezone.utc)
        return df
