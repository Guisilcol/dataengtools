from typing import List, Optional, TypeVar, Generic
from dataengtools.core.interfaces.engine_layer.catalog import CatalogEngine
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.integration_layer.catalog_metadata import TableMetadata, TableMetadataRetriver 
from dataengtools.core.interfaces.integration_layer.catalog_partitions import  Partition, PartitionHandler

T = TypeVar('T')


class CatalogTemplate(CatalogEngine[T], Generic[T]):
    """
    Template for a CatalogEngine implementation.
    This class does not implement readin methods on data structures such as DataFrames.
    It is meant to be extended by a concrete implementation that will define how to read data.
    The methods implemented here are meant to be common to all CatalogEngine implementations.
    """
    
    def __init__(self, 
                 partition_handler: PartitionHandler, 
                 table_metadata_retriver: TableMetadataRetriver,
                 filesystem: FilesystemHandler
    ):
        self.partition_handler = partition_handler
        self.table_metadata_retriver = table_metadata_retriver
        self.filesystem = filesystem
    
    def get_location(self, db: str, table: str) -> str:
        location = self.table_metadata_retriver.get_table_metadata(db, table).location
        return location.rstrip("/")

    def get_table_metadata(self, db: str, table: str) -> TableMetadata:
        return self.table_metadata_retriver.get_table_metadata(db, table)
    
    def get_partitions(self, db: str, table: str, conditions: Optional[str] = None) -> List[Partition]:
        return self.partition_handler.get_partitions(db, table, conditions)
    
    def get_partitions_columns(self, db: str, table: str) -> List[str]:
        cols = self.table_metadata_retriver.get_table_metadata(db, table).partition_columns
        return [c.name for c in cols]
    
    def repair_table(self, db: str, table: str) -> None:
        self.partition_handler.repair_table(db, table)
        
    def delete_partitions(self, db: str, table: str, partitions: Optional[List[Partition]] = None) -> None:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        location = metadata.location

        if not partitions:
            partitions = self.partition_handler.get_partitions(db, table)

        for p in partitions:
            partition_location = f"{location}/{p}"
            files = self.filesystem.get_files(partition_location)
            self.filesystem.delete_files(files)

        self.partition_handler.delete_partitions(db, table, partitions)

    def truncate_table(self, db: str, table: str) -> None:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)

        if metadata.partition_columns:
            self.delete_partitions(db, table)
        
        files = self.filesystem.get_files(metadata.location)
        self.filesystem.delete_files(files)

    def read_table(self, db, table, columns = None):
        raise NotImplementedError("This class not have a concrete implementation of this method")
    
    def read_partitioned_table(self, db, table, conditions, columns = None):
        raise NotImplementedError("This class not have a concrete implementation of this method")
    
    def adapt_frame_to_table_schema(self, df, db, table):
        raise NotImplementedError("This class not have a concrete implementation of this method")
    
    def write_table(self, df, db, table, overwrite, compreesion = None):
        raise NotImplementedError("This class not have a concrete implementation of this method")
