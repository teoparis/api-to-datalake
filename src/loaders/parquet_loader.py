"""
ParquetLoader — serialise a DataFrame to Parquet and upload to ADLS Gen2.

Upload path format::

    {base_path}/{target_path}/year={Y}/month={MM}/day={DD}/{source}_{ts}.parquet

Authentication priority:
1. ``ADLS_CONNECTION_STRING`` env var — direct connection string
2. ``DefaultAzureCredential`` — works with Managed Identity, az login, env vars

Requires: ``azure-storage-file-datalake``, ``pyarrow``
"""

import io
import os
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from src.utils.logger import get_logger

log = get_logger(__name__)

_DEFAULT_COMPRESSION = "snappy"


def _build_adls_path(
    base_path: str,
    target_path: str,
    source_name: str,
    ts: datetime,
) -> str:
    """Build the Hive-style partition path for the Parquet file.

    Args:
        base_path: Root path inside the ADLS container (e.g. ``"raw"``).
        target_path: Source-specific sub-path (e.g. ``"exchange_rates"``).
        source_name: Source identifier used as filename prefix.
        ts: Timestamp for the partition and filename.

    Returns:
        Full path string, e.g.
        ``"raw/exchange_rates/year=2025/month=04/day=06/exchange_rates_20250406T120000Z.parquet"``
    """
    ts_tag = ts.strftime("%Y%m%dT%H%M%SZ")
    year = ts.strftime("%Y")
    month = ts.strftime("%m")
    day = ts.strftime("%d")
    filename = f"{source_name}_{ts_tag}.parquet"
    return f"{base_path}/{target_path}/year={year}/month={month}/day={day}/{filename}"


def _df_to_parquet_bytes(df: pd.DataFrame, compression: str = _DEFAULT_COMPRESSION) -> bytes:
    """Serialise *df* to Parquet format in memory.

    Args:
        df: DataFrame to serialise.
        compression: Parquet compression codec (default ``"snappy"``).

    Returns:
        Raw Parquet bytes.
    """
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=compression)
    buf.seek(0)
    return buf.read()


def _get_service_client(account_name: str) -> DataLakeServiceClient:
    """Instantiate a DataLakeServiceClient using the best available credential.

    Checks ``ADLS_CONNECTION_STRING`` first; falls back to
    ``DefaultAzureCredential`` (Managed Identity / az login / env vars).

    Args:
        account_name: Storage account name (used for URL construction).

    Returns:
        Authenticated :class:`~azure.storage.filedatalake.DataLakeServiceClient`.
    """
    conn_str = os.environ.get("ADLS_CONNECTION_STRING")
    if conn_str:
        log.debug("Using ADLS connection string auth")
        return DataLakeServiceClient.from_connection_string(conn_str)

    account_url = f"https://{account_name}.dfs.core.windows.net"
    log.debug("Using DefaultAzureCredential for ADLS", account_url=account_url)
    credential = DefaultAzureCredential()
    return DataLakeServiceClient(account_url=account_url, credential=credential)


class ParquetLoader:
    """Upload a DataFrame as a Parquet file to Azure Data Lake Storage Gen2.

    Args:
        container_name: ADLS filesystem (container) name.
        account_name: Storage account name. Falls back to env var
                      ``ADLS_ACCOUNT_NAME`` if not provided.
        base_path: Root directory inside the container.
                   Defaults to ``ADLS_BASE_PATH`` env var or ``"raw"``.
        compression: Parquet codec — ``"snappy"`` (default), ``"gzip"``,
                     ``"brotli"``, or ``"none"``.
    """

    def __init__(
        self,
        container_name: str,
        account_name: Optional[str] = None,
        base_path: Optional[str] = None,
        compression: str = _DEFAULT_COMPRESSION,
    ) -> None:
        self._container = container_name
        self._account_name = account_name or os.environ.get("ADLS_ACCOUNT_NAME", "")
        self._base_path = (
            base_path
            or os.environ.get("ADLS_BASE_PATH", "raw")
        ).rstrip("/")
        self._compression = compression
        log.debug(
            "ParquetLoader initialised",
            container=self._container,
            account=self._account_name,
            base_path=self._base_path,
        )

    def load(
        self,
        df: pd.DataFrame,
        source_name: str,
        target_path: str,
        upload_ts: Optional[datetime] = None,
    ) -> dict:
        """Serialise *df* to Parquet and upload it to ADLS Gen2.

        Args:
            df: DataFrame to upload.
            source_name: Source identifier (used in the filename).
            target_path: Sub-path appended after ``base_path`` in ADLS
                         (e.g. ``"raw/exchange_rates"``).
            upload_ts: Override the upload timestamp (UTC).  Defaults to now.

        Returns:
            Dict with keys ``path``, ``size_bytes``, ``row_count``,
            ``duration_ms``.  If *df* is empty returns ``{rows: 0}`` without
            uploading.
        """
        if df.empty:
            log.info("DataFrame is empty — skipping upload", source=source_name)
            return {"rows": 0, "path": None, "size_bytes": 0, "duration_ms": 0}

        ts = upload_ts or datetime.now(timezone.utc)
        adls_path = _build_adls_path(
            base_path=self._base_path,
            target_path=target_path,
            source_name=source_name,
            ts=ts,
        )

        log.info(
            "Serialising DataFrame to Parquet",
            rows=len(df),
            columns=list(df.columns),
            compression=self._compression,
        )

        t_start = time.monotonic()
        parquet_bytes = _df_to_parquet_bytes(df, self._compression)
        size_bytes = len(parquet_bytes)

        log.info(
            "Uploading to ADLS",
            path=adls_path,
            size_bytes=size_bytes,
            container=self._container,
        )

        service_client = _get_service_client(self._account_name)
        fs_client = service_client.get_file_system_client(self._container)

        # Ensure parent directories exist
        dir_path = "/".join(adls_path.split("/")[:-1])
        try:
            fs_client.create_directory(dir_path)
        except Exception:
            pass  # directory may already exist — that's fine

        file_client = fs_client.get_file_client(adls_path)
        file_client.upload_data(parquet_bytes, overwrite=True, length=size_bytes)

        # Verify uploaded file size
        props = file_client.get_file_properties()
        uploaded_size = props["size"]
        duration_ms = int((time.monotonic() - t_start) * 1000)

        if uploaded_size != size_bytes:
            raise RuntimeError(
                f"Upload size mismatch for {adls_path}: "
                f"expected {size_bytes}, got {uploaded_size}"
            )

        log.info(
            "Upload verified",
            path=adls_path,
            size_bytes=uploaded_size,
            row_count=len(df),
            duration_ms=duration_ms,
        )

        return {
            "path": adls_path,
            "size_bytes": uploaded_size,
            "row_count": len(df),
            "duration_ms": duration_ms,
        }
