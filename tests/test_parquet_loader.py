import pytest
import io
from unittest.mock import patch, MagicMock
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date
from src.loaders.parquet_loader import ParquetLoader


@pytest.fixture
def loader():
    return ParquetLoader(
        storage_account="testaccount",
        container="datalake",
        base_path="raw",
        credential="fake-conn-string",
    )


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "transaction_id": ["t1", "t2", "t3"],
        "amount": [100.0, 200.5, 50.0],
        "currency": ["EUR", "USD", "EUR"],
        "created_at": pd.to_datetime(["2024-03-15", "2024-03-15", "2024-03-15"]),
    })


class TestParquetLoaderPath:
    def test_partition_path_contains_year_month_day(self, loader):
        target_date = date(2024, 3, 15)
        path = loader._build_path("transactions", target_date)
        assert "year=2024" in path
        assert "month=03" in path
        assert "day=15" in path

    def test_partition_path_contains_source_name(self, loader):
        path = loader._build_path("bank_transactions", date(2024, 1, 1))
        assert "bank_transactions" in path

    def test_partition_path_starts_with_base_path(self, loader):
        path = loader._build_path("events", date(2024, 6, 1))
        assert path.startswith("raw/")


class TestParquetSerialization:
    def test_dataframe_serializes_to_valid_parquet(self, loader, sample_df):
        buffer = loader._serialize_to_parquet(sample_df)
        assert isinstance(buffer, bytes)
        # verify it can be read back
        result = pd.read_parquet(io.BytesIO(buffer))
        assert len(result) == len(sample_df)
        assert list(result.columns) == list(sample_df.columns)

    def test_parquet_uses_snappy_compression(self, loader, sample_df):
        buffer = loader._serialize_to_parquet(sample_df)
        pf = pq.ParquetFile(io.BytesIO(buffer))
        assert pf.metadata.row_group(0).column(0).compression == "SNAPPY"


class TestParquetLoaderEmptyDataFrame:
    @patch("src.loaders.parquet_loader.DataLakeServiceClient")
    def test_empty_dataframe_skips_upload(self, mock_client_cls, loader):
        empty_df = pd.DataFrame()
        result = loader.load(empty_df, source_name="transactions", target_date=date(2024, 3, 15))

        assert result["row_count"] == 0
        assert result["status"] == "skipped"
        mock_client_cls.return_value.get_file_system_client.assert_not_called()


class TestParquetLoaderUpload:
    @patch("src.loaders.parquet_loader.DataLakeServiceClient")
    def test_upload_called_with_correct_path(self, mock_client_cls, loader, sample_df):
        mock_fs = MagicMock()
        mock_client_cls.return_value.get_file_system_client.return_value = mock_fs
        mock_file = MagicMock()
        mock_fs.get_file_client.return_value = mock_file
        mock_file.get_file_properties.return_value.size = 1024

        result = loader.load(sample_df, source_name="transactions", target_date=date(2024, 3, 15))

        assert result["row_count"] == 3
        assert result["status"] == "ok"
        assert "year=2024" in result["path"]
        mock_file.upload_data.assert_called_once()
