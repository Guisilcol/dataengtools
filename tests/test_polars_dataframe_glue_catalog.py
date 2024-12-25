import pytest
import polars as pl
from unittest.mock import MagicMock
from uuid import uuid4
import tempfile

from dataengtools.interfaces.metadata import TableMetadata, Partition, Column
from dataengtools.polars_integration.catalog import DataFrameGlueCatalog

@pytest.fixture
def mock_partition_handler() -> MagicMock:
    return MagicMock()

@pytest.fixture
def mock_table_metadata_retriver() -> MagicMock:
    return MagicMock()

@pytest.fixture
def mock_file_handler() -> MagicMock:
    return MagicMock()

@pytest.fixture
def glue_catalog(mock_partition_handler, mock_table_metadata_retriver, mock_file_handler) -> DataFrameGlueCatalog:
    return DataFrameGlueCatalog(
        partition_handler=mock_partition_handler,
        table_metadata_retriver=mock_table_metadata_retriver,
        file_handler=mock_file_handler
    )

class TestDataFrameGlueCatalog:

    def test_get_location(self, glue_catalog: DataFrameGlueCatalog, mock_table_metadata_retriver: MagicMock):
        mock_table_metadata_retriver.get_table_metadata.return_value = TableMetadata(
            database="db",
            table="table",
            location="s3://mock-bucket/mock-path/",
            columns=[],
            partition_columns=[],
            all_columns=[],
            files_have_header=False,
            files_extension="parquet",
            columns_separator=",",
            raw_metadata={},
            source="AWS Glue"
        )

        location = glue_catalog.get_location("db", "table")
        assert location == "s3://mock-bucket/mock-path"

    def test_read_table_parquet(self, glue_catalog: DataFrameGlueCatalog, mock_table_metadata_retriver: MagicMock, monkeypatch):
        mock_table_metadata_retriver.get_table_metadata.return_value = TableMetadata(
            database="db",
            table="table",
            location="s3://mock-bucket/mock-path/",
            columns=[Column(name="col1", datatype="int"), Column(name="col2", datatype="int")],
            partition_columns=[],
            all_columns=[Column(name="col1", datatype="int"), Column(name="col2", datatype="int")],
            files_have_header=False,
            files_extension="parquet",
            columns_separator=",",
            raw_metadata={},
            source="AWS Glue"
        )
        
        mock_read_parquet = MagicMock(return_value=pl.DataFrame({"col1": [1, 2], "col2": [3, 4]}))
        monkeypatch.setattr(pl, "read_parquet", mock_read_parquet)

        df = glue_catalog.read_table("db", "table", ["col1", "col2"])

        mock_read_parquet.assert_called_once_with("s3://mock-bucket/mock-path/**", columns=["col1", "col2"])
        assert df.shape == (2, 2)

    def test_read_table_csv(self, glue_catalog: DataFrameGlueCatalog, mock_table_metadata_retriver: MagicMock, monkeypatch):
        mock_table_metadata_retriver.get_table_metadata.return_value = TableMetadata(
            database="db",
            table="table",
            location="s3://mock-bucket/mock-path/",
            columns=["col1", "col2"],
            partition_columns=[],
            all_columns=[],
            files_have_header=True,
            files_extension="csv",
            columns_separator=",",
            raw_metadata={},
            source="AWS Glue"
        )

        mock_read_csv = MagicMock(return_value=pl.DataFrame({"col1": [1, 2], "col2": [3, 4]}))
        monkeypatch.setattr(pl, "read_csv", mock_read_csv)

        df = glue_catalog.read_table("db", "table", ["col1", "col2"])

        mock_read_csv.assert_called_once_with(
            "s3://mock-bucket/mock-path/**",
            separator=",",
            has_header=True,
            columns=["col1", "col2"]
        )
        assert df.shape == (2, 2)

    def test_get_partitions(self, glue_catalog: DataFrameGlueCatalog, mock_partition_handler: MagicMock):
        mock_partition_handler.get_partitions.return_value = [
            Partition(
                location="s3://mock-bucket/mock-path/partition1=2024-01-01/", 
                values=["2024-01-01"], 
                name="mock-path/partition1=2024-01-01"
            ),
            Partition(
                location="s3://mock-bucket/mock-path/partition1=2024-01-02/", 
                values=["2024-01-02"], 
                name="mock-path/partition1=2024-01-02"
            )
        ]

        partitions = glue_catalog.get_partitions("db", "table", "conditions")

        assert len(partitions) == 2
        assert partitions[0].location == "s3://mock-bucket/mock-path/partition1=2024-01-01/"
        assert partitions[1].location == "s3://mock-bucket/mock-path/partition1=2024-01-02/"
        assert partitions[0].values == ["2024-01-01"]
        assert partitions[1].values == ["2024-01-02"]
        assert partitions[0].name == "mock-path/partition1=2024-01-01"
        assert partitions[1].name == "mock-path/partition1=2024-01-02"

    def test_write_table_overwrite(self, glue_catalog: DataFrameGlueCatalog, mock_table_metadata_retriver: MagicMock, mock_file_handler: MagicMock, monkeypatch):
        temp_dir = tempfile.mkdtemp()
        
        mock_table_metadata_retriver.get_table_metadata.return_value = TableMetadata(
            database="db",
            table="table",
            location=temp_dir,
            columns=[],
            partition_columns=[],
            all_columns=[],
            files_have_header=True,
            files_extension="csv",
            columns_separator=",",
            raw_metadata={},
            source="AWS Glue"
        )

        mock_uuid = MagicMock(return_value=uuid4())
        monkeypatch.setattr("uuid.uuid4", mock_uuid)

        glue_catalog.write_table(
            df=pl.DataFrame({"col1": [1, 2], "col2": [3, 4]}),
            db="db",
            table="table",
            overwrite=True
        )

        mock_file_handler.delete_files.assert_called()
        mock_file_handler.get_files.assert_called()

    def test_adapt_frame_to_table_schema(self, glue_catalog: DataFrameGlueCatalog, mock_table_metadata_retriver: MagicMock):
        mock_table_metadata_retriver.get_table_metadata.return_value = TableMetadata(
            database="db",
            table="table",
            location="s3://mock-bucket/mock-path/",
            columns=[
                Column(name="col1", datatype="int"), 
                Column(name="col2", datatype="int")
            ],
            partition_columns=[],
            all_columns=[
                Column(name="col1", datatype="int"), 
                Column(name="col2", datatype="int")
            ],
            files_have_header=True,
            files_extension="csv",
            columns_separator=",",
            raw_metadata={},
            source="AWS Glue"
        )

        df = pl.DataFrame({"col1": [1, 2]})
        adapted_df = glue_catalog.adapt_frame_to_table_schema(df, "db", "table")

        assert "col2" in adapted_df.columns
