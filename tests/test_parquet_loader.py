"""
tests/test_parquet_loader.py — unit tests for ParquetLoader.

Tests:
* path generation   — Hive-style partitioning with correct year/month/day
* serialisation     — bytes written to ADLS are valid Parquet (round-trip check)
* empty DataFrame   — upload is skipped, returns {rows: 0}
* size verification — RuntimeError raised if uploaded size != local size
"""

import io
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.loaders.parquet_loader import ParquetLoader, _build_adls_path, _df_to_parquet_bytes


# ── Test: path generation ─────────────────────────────────────────────────────

class TestPathGeneration:
    """_build_adls_path must produce correctly partitioned paths."""

    def test_hive_partition_format(self):
        ts = datetime(2025, 4, 6, 12, 30, 0, tzinfo=timezone.utc)
        path = _build_adls_path(
            base_path="raw",
            target_path="exchange_rates",
            source_name="exchange_rates",
            ts=ts,
        )
        assert "year=2025" in path
        assert "month=04" in path
        assert "day=06" in path
        assert path.endswith(".parquet")
        assert path.startswith("raw/exchange_rates/")

    def test_filename_contains_source_and_timestamp(self):
        ts = datetime(2025, 4, 6, 9, 0, 0, tzinfo=timezone.utc)
        path = _build_adls_path("raw", "transactions", "bank_transactions", ts)
        filename = path.split("/")[-1]
        assert filename.startswith("bank_transactions_")
        assert "20250406T090000Z" in filename

    def test_different_months_produce_different_paths(self):
        ts_jan = datetime(2025, 1, 1, tzinfo=timezone.utc)
        ts_dec = datetime(2025, 12, 1, tzinfo=timezone.utc)
        p1 = _build_adls_path("raw", "src", "s", ts_jan)
        p2 = _build_adls_path("raw", "src", "s", ts_dec)
        assert "month=01" in p1
        assert "month=12" in p2
        assert p1 != p2


# ── Test: serialisation / round-trip ─────────────────────────────────────────

class TestParquetSerialisation:
    """Bytes produced by _df_to_parquet_bytes must be valid Parquet."""

    def test_round_trip(self):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["alpha", "beta", "gamma"],
            "value": [1.1, 2.2, 3.3],
        })
        parquet_bytes = _df_to_parquet_bytes(df)
        assert isinstance(parquet_bytes, bytes)
        assert len(parquet_bytes) > 0

        # Read back and compare
        recovered = pd.read_parquet(io.BytesIO(parquet_bytes))
        assert list(recovered.columns) == list(df.columns)
        assert len(recovered) == len(df)
        assert list(recovered["id"]) == [1, 2, 3]

    def test_snappy_compression(self):
        df = pd.DataFrame({"x": range(100)})
        parquet_bytes = _df_to_parquet_bytes(df, compression="snappy")
        # Verify it is valid Parquet
        table = pq.read_table(io.BytesIO(parquet_bytes))
        assert table.num_rows == 100

    def test_column_types_preserved(self):
        df = pd.DataFrame({
            "int_col": pd.array([1, 2], dtype="int64"),
            "float_col": [3.14, 2.71],
            "str_col": ["a", "b"],
        })
        parquet_bytes = _df_to_parquet_bytes(df)
        recovered = pd.read_parquet(io.BytesIO(parquet_bytes))
        assert recovered["int_col"].dtype == "int64"
        assert recovered["float_col"].dtype == "float64"


# ── Test: empty DataFrame ─────────────────────────────────────────────────────

class TestEmptyDataFrame:
    """Empty DataFrames must be skipped -- no upload, return rows=0."""

    def test_empty_df_skips_upload(self, monkeypatch):
        monkeypatch.setenv("ADLS_ACCOUNT_NAME", "testaccount")

        loader = ParquetLoader(container_name="datalake")

        with patch("src.loaders.parquet_loader._get_service_client") as mock_client:
            result = loader.load(
                df=pd.DataFrame(),
                source_name="empty_source",
                target_path="raw/empty",
            )

        mock_client.assert_not_called()
        assert result["rows"] == 0
        assert result["path"] is None

    def test_empty_df_returns_zero_size(self):
        loader = ParquetLoader(container_name="datalake")
        with patch("src.loaders.parquet_loader._get_service_client"):
            result = loader.load(pd.DataFrame(), "s", "raw/s")
        assert result["size_bytes"] == 0
        assert result["duration_ms"] == 0


# ── Test: ADLS upload ─────────────────────────────────────────────────────────

class TestAdlsUpload:
    """Verify that load() calls the ADLS SDK correctly."""

    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "id": [10, 20, 30],
            "amount": [1.5, 2.5, 3.5],
        })

    def _mock_adls(self, uploaded_size: int):
        """Return a mock DataLakeServiceClient chain."""
        file_props = {"size": uploaded_size}
        file_client = MagicMock()
        file_client.get_file_properties.return_value = file_props

        dir_client = MagicMock()
        fs_client = MagicMock()
        fs_client.get_file_client.return_value = file_client
        fs_client.create_directory.return_value = dir_client

        service_client = MagicMock()
        service_client.get_file_system_client.return_value = fs_client
        return service_client, fs_client, file_client

    def test_upload_called_with_correct_data(self, monkeypatch):
        """upload_data is called once with the serialised Parquet bytes."""
        monkeypatch.setenv("ADLS_ACCOUNT_NAME", "myaccount")
        df = self._make_df()
        expected_bytes = _df_to_parquet_bytes(df)

        service_client, fs_client, file_client = self._mock_adls(len(expected_bytes))

        with patch("src.loaders.parquet_loader._get_service_client", return_value=service_client):
            loader = ParquetLoader(container_name="datalake")
            ts = datetime(2025, 6, 15, tzinfo=timezone.utc)
            result = loader.load(df, "test_source", "raw/test", upload_ts=ts)

        file_client.upload_data.assert_called_once()
        call_args = file_client.upload_data.call_args
        uploaded_data = call_args[0][0]  # first positional arg
        assert isinstance(uploaded_data, bytes)
        # Verify uploaded bytes are valid Parquet
        recovered = pd.read_parquet(io.BytesIO(uploaded_data))
        assert len(recovered) == 3

    def test_result_contains_expected_keys(self, monkeypatch):
        """load() result dict has path, size_bytes, row_count, duration_ms."""
        monkeypatch.setenv("ADLS_ACCOUNT_NAME", "acct")
        df = self._make_df()
        size = len(_df_to_parquet_bytes(df))
        service_client, _, _ = self._mock_adls(size)

        with patch("src.loaders.parquet_loader._get_service_client", return_value=service_client):
            loader = ParquetLoader(container_name="datalake")
            result = loader.load(
                df, "src1", "raw/src1",
                upload_ts=datetime(2025, 4, 6, tzinfo=timezone.utc),
            )

        assert "path" in result
        assert result["row_count"] == 3
        assert result["size_bytes"] == size
        assert isinstance(result["duration_ms"], int)

    def test_path_in_result_contains_partition(self, monkeypatch):
        """The returned path must contain correct Hive partitions."""
        monkeypatch.setenv("ADLS_ACCOUNT_NAME", "acct")
        df = self._make_df()
        size = len(_df_to_parquet_bytes(df))
        service_client, _, _ = self._mock_adls(size)
        ts = datetime(2025, 3, 22, tzinfo=timezone.utc)

        with patch("src.loaders.parquet_loader._get_service_client", return_value=service_client):
            loader = ParquetLoader(container_name="datalake")
            result = loader.load(df, "orders", "raw/orders", upload_ts=ts)

        assert "year=2025" in result["path"]
        assert "month=03" in result["path"]
        assert "day=22" in result["path"]

    def test_size_mismatch_raises_runtime_error(self, monkeypatch):
        """RuntimeError raised when ADLS reports a different size than local."""
        monkeypatch.setenv("ADLS_ACCOUNT_NAME", "acct")
        df = self._make_df()
        # ADLS reports a wrong size
        service_client, _, _ = self._mock_adls(uploaded_size=1)

        with patch("src.loaders.parquet_loader._get_service_client", return_value=service_client):
            loader = ParquetLoader(container_name="datalake")
            with pytest.raises(RuntimeError, match="Upload size mismatch"):
                loader.load(df, "s", "raw/s", upload_ts=datetime(2025, 4, 6, tzinfo=timezone.utc))
