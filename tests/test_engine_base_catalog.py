from typing import List
import pytest
from unittest.mock import Mock

from dataengtools.io.polars.base_catalog import CatalogTemplate
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.integration_layer.catalog_metadata import (
    TableMetadata, 
    TableMetadataRetriver, 
    Column
)
from dataengtools.core.interfaces.integration_layer.catalog_partitions import (
    Partition, 
    PartitionHandler
)


class TestCatalogTemplate:
    @pytest.fixture
    def partition_handler(self) -> PartitionHandler:
        return Mock(spec=PartitionHandler)

    @pytest.fixture
    def table_metadata_retriver(self) -> TableMetadataRetriver:
        return Mock(spec=TableMetadataRetriver)

    @pytest.fixture
    def filesystem(self) -> FilesystemHandler:
        return Mock(spec=FilesystemHandler)

    @pytest.fixture
    def catalog(
        self, 
        partition_handler: PartitionHandler, 
        table_metadata_retriver: TableMetadataRetriver,
        filesystem: FilesystemHandler
    ) -> CatalogTemplate:
        return CatalogTemplate(
            partition_handler=partition_handler,
            table_metadata_retriver=table_metadata_retriver,
            filesystem=filesystem
        )

    @pytest.fixture
    def sample_table_metadata(self) -> TableMetadata:
        columns = [
            Column(name="id", datatype="int"),
            Column(name="value", datatype="double")
        ]
        partition_columns = [
            Column(name="year", datatype="string"),
            Column(name="month", datatype="string")
        ]
        all_columns = columns + partition_columns
        
        return TableMetadata(
            database="test_db",
            table="test_table",
            columns=columns,
            partition_columns=partition_columns,
            all_columns=all_columns,
            location="/data/test_db/test_table",
            files_have_header=True,
            files_extension="parquet",
            columns_separator=",",
            raw_metadata={},
            source="test_source"
        )

    def test_get_location(
        self, 
        catalog: CatalogTemplate, 
        table_metadata_retriver: TableMetadataRetriver, 
        sample_table_metadata: TableMetadata
    ) -> None:
        # Arrange
        db: str = "test_db"
        table: str = "test_table"
        table_metadata_retriver.get_table_metadata.return_value = sample_table_metadata
        
        # Act
        location: str = catalog.get_location(db, table)
        
        # Assert
        assert location == "/data/test_db/test_table"
        table_metadata_retriver.get_table_metadata.assert_called_once_with(db, table)

    def test_get_location_strips_trailing_slash(
        self, 
        catalog: CatalogTemplate, 
        table_metadata_retriver: TableMetadataRetriver
    ) -> None:
        # Arrange
        metadata: TableMetadata = TableMetadata(
            database="test_db",
            table="test_table",
            columns=[],
            partition_columns=[],
            all_columns=[],
            location="/data/test_db/test_table/",
            files_have_header=True,
            files_extension="parquet",
            columns_separator=",",
            raw_metadata={},
            source="test_source"
        )
        table_metadata_retriver.get_table_metadata.return_value = metadata
        
        # Act
        location: str = catalog.get_location("test_db", "test_table")
        
        # Assert
        assert location == "/data/test_db/test_table"

    def test_get_table_metadata(
        self, 
        catalog: CatalogTemplate, 
        table_metadata_retriver: TableMetadataRetriver, 
        sample_table_metadata: TableMetadata
    ) -> None:
        # Arrange
        db: str = "test_db"
        table: str = "test_table"
        table_metadata_retriver.get_table_metadata.return_value = sample_table_metadata
        
        # Act
        metadata: TableMetadata = catalog.get_table_metadata(db, table)
        
        # Assert
        assert metadata == sample_table_metadata
        table_metadata_retriver.get_table_metadata.assert_called_once_with(db, table)

    def test_get_partitions(
        self, 
        catalog: CatalogTemplate, 
        partition_handler: PartitionHandler
    ) -> None:
        # Arrange
        db: str = "test_db"
        table: str = "test_table"
        conditions: str = "year=2023"
        expected_partitions: List[Partition] = [
            Partition({"year": "2023", "month": "01"}),
            Partition({"year": "2023", "month": "02"})
        ]
        partition_handler.get_partitions.return_value = expected_partitions
        
        # Act
        partitions: List[Partition] = catalog.get_partitions(db, table, conditions)
        
        # Assert
        assert partitions == expected_partitions
        partition_handler.get_partitions.assert_called_once_with(db, table, conditions)

    def test_get_partitions_columns(
        self, 
        catalog: CatalogTemplate, 
        table_metadata_retriver: TableMetadataRetriver, 
        sample_table_metadata: TableMetadata
    ) -> None:
        # Arrange
        db: str = "test_db"
        table: str = "test_table"
        table_metadata_retriver.get_table_metadata.return_value = sample_table_metadata
        
        # Act
        partition_columns: List[str] = catalog.get_partitions_columns(db, table)
        
        # Assert
        assert partition_columns == ["year", "month"]
        table_metadata_retriver.get_table_metadata.assert_called_once_with(db, table)

    def test_repair_table(
        self, 
        catalog: CatalogTemplate, 
        partition_handler: PartitionHandler
    ) -> None:
        # Arrange
        db: str = "test_db"
        table: str = "test_table"
        
        # Act
        catalog.repair_table(db, table)
        
        # Assert
        partition_handler.repair_table.assert_called_once_with(db, table)

    def test_delete_partitions(
        self, 
        catalog: CatalogTemplate, 
        partition_handler: PartitionHandler, 
        filesystem: FilesystemHandler, 
        table_metadata_retriver: TableMetadataRetriver, 
        sample_table_metadata: TableMetadata
    ) -> None:
        # Arrange
        db: str = "test_db"
        table: str = "test_table"
        partitions: List[Partition] = [
            Partition({"year": "2023", "month": "01"}),
            Partition({"year": "2023", "month": "02"})
        ]
        table_metadata_retriver.get_table_metadata.return_value = sample_table_metadata
        filesystem.get_files.return_value = ["/data/test_db/test_table/file1.parquet"]
        
        # Act
        catalog.delete_partitions(db, table, partitions)
        
        # Assert
        assert filesystem.get_files.call_count == len(partitions)
        assert filesystem.delete_files.call_count == len(partitions)
        partition_handler.delete_partitions.assert_called_once_with(db, table, partitions)