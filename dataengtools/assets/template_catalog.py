from abc import ABC
from typing import List, Optional, TypeVar, Generic
from dataengtools.interfaces.catalog import Catalog
from dataengtools.interfaces.filesystem import FilesystemHandler
from dataengtools.interfaces.metadata import TableMetadata, Partition, PartitionHandler, TableMetadataRetriver
from dataengtools.logger import Logger

LOGGER = Logger.get_instance()
T = TypeVar('T')


class CatalogTemplate(Catalog[T], Generic[T], ABC):
    """
    Template for a Catalog implementation.
    This class does not implement readin methods on data structures such as DataFrames.
    It is meant to be extended by a concrete implementation that will define how to read data.
    The methods implemented here are meant to be common to all Catalog implementations.
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
    
    def get_partitions(self, db: str, table: str, conditions: Optional[str]) -> List[Partition]:
        return self.partition_handler.get_partitions(db, table, conditions)
    
    def get_partition_columns(self, db: str, table: str) -> List[str]:
        cols = self.table_metadata_retriver.get_table_metadata(db, table).partition_columns
        return [c.name for c in cols]
    
    def repair_table(self, db: str, table: str) -> None:
        self.partition_handler.repair_table(db, table)
        
    def delete_partitions(self, db: str, table: str, partitions: List[Partition]) -> None:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        partitions = [p for p in partitions if p.location != metadata.location]
        
        for p in partitions:
            LOGGER.info(f'Getting files to delete from partition {p.name}: root={p.root}, location={p.name}')
            files = self.filesystem.get_filepaths(p.root, p.name)
            LOGGER.info(f"Deleting files from partition {p.name}: {files}")
            self.filesystem.delete_files(p.root, files)
            
        self.partition_handler.delete_partitions(db, table, partitions)
