from dataengtools.engines.polars.dataframe_catalog import PolarsDataFrameCatalog
import pytest
import polars as pl
from unittest.mock import Mock, patch
import os
import tempfile
import shutil
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.integration_layer.catalog_partitions import PartitionHandler
from dataengtools.core.interfaces.integration_layer.catalog_metadata import (
    TableMetadataRetriver, DataTypeMapping, TableMetadata, Column
)

class TestPolarsDataFrameCatalog:
    @pytest.fixture
    def temp_dir(self):
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_handlers(self):
        partition_handler = Mock(spec=PartitionHandler)
        table_metadata_retriver = Mock(spec=TableMetadataRetriver)
        filesystem = Mock(spec=FilesystemHandler)
        datatype_mapping = Mock(spec=DataTypeMapping)
        datatype_mapping.get.return_value = pl.Int64
        
        return (partition_handler, table_metadata_retriver, filesystem, datatype_mapping)

    @pytest.fixture
    def catalog(self, mock_handlers):
        partition_handler, table_metadata_retriver, filesystem, datatype_mapping = mock_handlers
        return PolarsDataFrameCatalog(
            partition_handler=partition_handler,
            table_metadata_retriver=table_metadata_retriver,
            filesystem=filesystem,
            datatype_mapping=datatype_mapping
        )

    def test_read_table_parquet(self, catalog, mock_handlers, temp_dir):
        _, table_metadata_retriver, _, _ = mock_handlers
        
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        test_file = os.path.join(temp_dir, "test.parquet")
        df.write_parquet(test_file)
        
        metadata = TableMetadata(
            database="test_db", table="test_table",
            columns=[
                Column(name="id", datatype="integer"),
                Column(name="name", datatype="string")
            ],
            partition_columns=[],
            all_columns=[],
            location=temp_dir,
            files_have_header=True,
            files_extension="parquet",
            columns_separator=","
        )
        table_metadata_retriver.get_table_metadata.return_value = metadata

        result = catalog.read_table("test_db", "test_table")

        assert isinstance(result, pl.DataFrame)
        assert result.shape == (3, 2)
        assert result.columns == ["id", "name"]

    def test_read_partitioned_table(self, catalog, mock_handlers, temp_dir):
        partition_handler, table_metadata_retriver, filesystem, _ = mock_handlers
        
        os.makedirs(os.path.join(temp_dir, "year=2023"))
        df = pl.DataFrame({"id": [1, 2], "value": [10, 20]})
        df.write_parquet(os.path.join(temp_dir, "year=2023/data.parquet"))

        metadata = TableMetadata(
            database="test_db", table="test_table",
            columns=[Column(name="id", datatype="integer")],
            partition_columns=[Column(name="year", datatype="integer")],
            all_columns=[],
            location=temp_dir,
            files_have_header=True,
            files_extension="parquet",
            columns_separator=","
        )
        table_metadata_retriver.get_table_metadata.return_value = metadata
        partition_handler.get_partitions.return_value = ["year=2023"]

        result = catalog.read_partitioned_table("test_db", "test_table", "year=2023")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2

    def test_adapt_frame_to_table_schema(self, catalog, mock_handlers):
        _, table_metadata_retriver, _, datatype_mapping = mock_handlers
        
        input_df = pl.DataFrame({
            "id": [1, 2, 3],
            "extra_col": ["a", "b", "c"]
        })

        metadata = TableMetadata(
            database="test_db", table="test_table",
            columns=[
                Column(name="id", datatype="integer"),
                Column(name="name", datatype="string")
            ],
            partition_columns=[],
            all_columns=[
                Column(name="id", datatype="integer"),
                Column(name="name", datatype="string")
            ],
            location="dummy",
            files_have_header=True,
            files_extension="parquet",
            columns_separator=None
        )
        table_metadata_retriver.get_table_metadata.return_value = metadata

        result = catalog.adapt_frame_to_table_schema(input_df, "test_db", "test_table")

        assert "id" in result.columns
        assert "name" in result.columns
        assert "extra_col" not in result.columns

    def test_write_table_non_partitioned(self, catalog, mock_handlers, temp_dir):
        _, table_metadata_retriver, filesystem, _ = mock_handlers

        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        metadata = TableMetadata(
            database="test_db", table="test_table",
            columns=[],
            partition_columns=[],
            all_columns=[],
            location=temp_dir,
            files_have_header=True,
            files_extension="parquet",
            columns_separator=","
        )

        table_metadata_retriver.get_table_metadata.return_value = metadata
        filesystem.open_file.return_value = open(os.path.join(temp_dir, "test.parquet"), "wb")

        catalog.write_table(df, "test_db", "test_table", overwrite=False)

        assert os.path.exists(os.path.join(temp_dir, "test.parquet"))

    def test_write_table_partitioned(self, catalog, mock_handlers, temp_dir):
        partition_handler, table_metadata_retriver, filesystem, _ = mock_handlers

        df = pl.DataFrame({
            "id": [1, 2], 
            "name": ["a", "b"],
            "year": [2023, 2024],
            "month": [1, 2]
        })

        metadata = TableMetadata(
            database="test_db", table="test_table",
            columns=[],
            partition_columns=[
                Column(name="year", datatype="integer"),
                Column(name="month", datatype="integer")
            ],
            all_columns=[],
            location=temp_dir,
            files_have_header=True,
            files_extension="parquet",
            columns_separator=","
        )

        table_metadata_retriver.get_table_metadata.return_value = metadata

        def _create_dir_if_not_exists_and_open_file(path, _):
            from pathlib import Path
            
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            return open(path, "wb")

        filesystem.open_file = _create_dir_if_not_exists_and_open_file
        
        catalog.write_table(df, "test_db", "test_table", overwrite=False)

        assert os.path.exists(os.path.join(temp_dir, "year=2023/month=1"))
        assert os.path.exists(os.path.join(temp_dir, "year=2024/month=2"))
         

    def test_unsupported_extension(self, catalog, mock_handlers):
        _, table_metadata_retriver, _, _ = mock_handlers

        metadata = TableMetadata(
            database="test_db", table="test_table",
            columns=[],
            partition_columns=[],
            all_columns=[],
            location="dummy",
            files_have_header=True,
            files_extension="txt",
            columns_separator=","
        )
        table_metadata_retriver.get_table_metadata.return_value = metadata

        with pytest.raises(ValueError, match="Unsupported files extension"):
            catalog.read_table("test_db", "test_table")
