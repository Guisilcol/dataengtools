from typing import Generator, List, Optional, Any
from dataengtools.core.interfaces.engine_layer.catalog import CatalogEngine
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.integration_layer.catalog_metadata import TableMetadata, TableMetadataRetriver 
from dataengtools.core.interfaces.integration_layer.catalog_partitions import  Partition, PartitionHandler
from dataengtools.core.interfaces.io.reader import Reader

from duckdb import DuckDBPyRelation


class DuckDBCatalogEngine(CatalogEngine[DuckDBPyRelation, Any]):    
    def __init__(self, 
                 partition_handler: PartitionHandler, 
                 table_metadata_retriver: TableMetadataRetriver,
                 filesystem: FilesystemHandler,
                 reader: Reader[DuckDBPyRelation]
    ):
        self.partition_handler = partition_handler
        self.table_metadata_retriver = table_metadata_retriver
        self.filesystem = filesystem
        self.reader = reader
        
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

    def read_table(
        self,         
        db: str, 
        table: str, 
        condition: Optional[str], 
        columns: Optional[List[str]] = None
    ) -> DuckDBPyRelation:
        metadata = self.get_table_metadata(db, table)
        
        data, _ = self.reader.read(
            path=metadata.location,
            columns=columns,
            file_type=metadata.files_extension,
            delimiter=metadata.columns_separator,
            have_header=metadata.files_have_header,
            condition=condition
        )
        return data
    
    def read_table_batched(
        self,
        db: str, 
        table: str, 
        condition: Optional[str], 
        columns: Optional[List[str]] = None, 
        order_by: Optional[List[str]] = None,
        batch_size: int = 10000
    ) -> Generator[DuckDBPyRelation, None, None]:
        metadata = self.get_table_metadata(db, table)
        
        current_offset = 0
        while True:
            batch, count = self.reader.read(
                path=metadata.location,
                columns=columns,
                file_type=metadata.files_extension,
                delimiter=metadata.columns_separator,
                have_header=metadata.files_have_header,
                condition=condition,
                offset=current_offset,
                limit=batch_size,
                order_by=order_by
            )
            
            if count == 0:
                break
            
            yield batch
            current_offset += batch_size
    
    def write_table(self, df, db, table, overwrite, compreesion = None):
        raise NotImplementedError("This class not have a concrete implementation of this method")

