from typing import List, Optional
import polars as pl
from uuid import uuid4

from dataengtools.interfaces.catalog import Catalog
from dataengtools.interfaces.metadata import TableMetadata, Partition, PartitionHandler, TableMetadataRetriver
from dataengtools.interfaces.filesystem import FilesystemOperationsHandler
from dataengtools.logger import Logger

LOGGER = Logger.get_instance()

class DataFrameGlueCatalog(Catalog[pl.DataFrame]):
    
    GLUE_DATATYPE_TO_POLARS = {
        'string': pl.Utf8,          # Correto: Texto UTF-8
        'int': pl.Int64,            # Correto: Inteiro de 64 bits
        'bigint': pl.Int64,         # Ajustado para Int64 (inteiro de 64 bits)
        'double': pl.Float64,       # Ajustado para Float64
        'float': pl.Float32,        # Ajustado para Float32
        'boolean': pl.Boolean,      # Ajustado para Boolean
        'timestamp': pl.Datetime,   # Ajustado para Datetime
        'date': pl.Date,            # Ajustado para Date
        'decimal': pl.Float64,      # Decimal representado como Float64
        'array': pl.List,           # Arrays mapeados para List
        'map': pl.Object,           # Map mapeado para Object
        'struct': pl.Object,        # Struct mapeado para Object
        'binary': pl.Binary         # Ajustado para Binary
    }

    
    def __init__(self, 
                 partition_handler: PartitionHandler, 
                 table_metadata_retriver: TableMetadataRetriver,
                 file_handler: FilesystemOperationsHandler
    ):
        self.partition_handler = partition_handler
        self.table_metadata_retriver = table_metadata_retriver
        self.file_handler = file_handler
    
    def get_location(self, db: str, table: str) -> str:
        location = self.table_metadata_retriver.get_table_metadata(db, table).location
        return location.rstrip("/")

    def get_table_metadata(self, db: str, table: str) -> TableMetadata:
        return self.table_metadata_retriver.get_table_metadata(db, table)
    
    def get_partitions(self, db: str, table: str, conditions: Optional[str]) -> List[Partition]:
        return self.partition_handler.get_partitions(db, table, conditions)
    
    def read_table(self, db: str, table: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        
        if metadata.files_extension == 'parquet':
            return pl.read_parquet(self.get_location(db, table) + '/**', columns=columns)
        
        if metadata.files_extension == 'csv':
            return pl.read_csv(
                self.get_location(db, table) + '/**', 
                separator=metadata.columns_separator,
                has_header=metadata.files_have_header,
                columns=columns
            )

        raise ValueError(f'Unsupported files extension {metadata.files_extension}')
    
    def read_partitioned_table(self, db: str, table: str, conditions: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        
        if not metadata.partition_columns:
            raise ValueError(f'Table {db}.{table} is not partitioned')
        
        partitions = self.partition_handler.get_partitions(db, table, conditions)
        
        if metadata.files_extension == 'parquet':
            dfs = []
            for partition in partitions:
                location = partition.location
                lf = pl.read_parquet(location + '/**', columns=columns)
                dfs.append(lf)
            
            return pl.concat(dfs)
        
        if metadata.files_extension == 'csv':
            dfs = []
            for partition in partitions:
                location = partition.location
                lf = pl.read_parquet(
                    location + '/**', 
                    separator=metadata.columns_separator,
                    has_header=metadata.files_have_header,
                    columns=columns
                )
                dfs.append(lf)
            
            return pl.concat(dfs)
        
        raise ValueError(f'Unsupported files extension {metadata.files_extension}')
        
    def adapt_frame_to_table_schema(self, df: pl.DataFrame, db: str, table: str) -> pl.DataFrame:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        
        for column in metadata.all_columns:
            if column.name not in df.columns:
                df = (
                    df
                    .with_columns(
                        pl.lit(None)
                        .cast(self.GLUE_DATATYPE_TO_POLARS.get(column.datatype, 'object'))
                        .alias(column.name)
                    )
                )
                
        df = df.select(*[c.name for c in metadata.all_columns])
        
        df = df.with_columns([
            pl.col(column.name)
                .cast(self.GLUE_DATATYPE_TO_POLARS.get(column.datatype, 'object')) 
            for column in metadata.columns
        ])

        return df
        
    def write_table(self, df: pl.DataFrame, db: str, table: str, overwrite: bool, compreesion: str = 'snappy') -> None:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        filename = str(uuid4()) + '.' + metadata.files_extension
        
        LOGGER.info(f'Writing DataFrame into {db}.{table} table')
        
        if not metadata.partition_columns:
            location = self.get_location(db, table)
            
            if overwrite:
                LOGGER.info(f'Overwrite flag is True. Deleting all {db}.{table} data.')
                bucket, prefix = metadata.location.replace('s3://', '').split('/', 1)
                files_to_delete = self.file_handler.get_filepaths(bucket, prefix)
                self.file_handler.delete_files(bucket, files_to_delete)
            
            filepath = location + '/' + filename
            
            LOGGER.info(f'Writing DataFrame in {filepath} file')
            
            with self.file_handler.open_file(filepath, 'wb') as f:
                if metadata.files_extension == 'parquet':
                    df.write_parquet(f, compression=compreesion)
                    return
                    
                if metadata.files_extension == 'csv':
                    df.write_csv(f, 
                                include_header=metadata.files_have_header, 
                                separator=metadata.columns_separator
                    )
                    return
                
                raise ValueError(f'Unsupported files extension {metadata.files_extension}')
            
        partition_columns = metadata.partition_columns
        
        for (partition_values, grouped_df) in df.group_by(*[p.name for p in partition_columns]):
            partition_name = '/'.join([f'{p.name}={v}' for p, v in zip(partition_columns, partition_values)])
            
            if overwrite:
                bucket, prefix = metadata.location.replace('s3://', '').split('/', 1)
                files_to_delete = self.file_handler.get_filepaths(bucket, prefix + '/' + partition_name)
                self.file_handler.delete_files(bucket, files_to_delete)
            
            location = self.get_location(db, table) + '/' + partition_name
            
            filepath = location + '/' + filename
            
            LOGGER.info(f'Writing Partitioned DataFrame in {filepath} file')
            
            with self.file_handler.open_file(filepath, 'wb') as f:
                if metadata.files_extension == 'parquet':
                    grouped_df.write_parquet(f, compression=compreesion)
                    continue

                if metadata.files_extension == 'csv':
                    grouped_df.write_csv(
                        f,
                        separator=metadata.columns_separator,
                        include_header=metadata.files_have_header
                    )
                    continue
                
                raise ValueError(f'Unsupported files extension {metadata.files_extension}')
            
        self.repair_table(db, table)
            
    def get_partition_columns(self, db: str, table: str) -> List[str]:
        cols = self.table_metadata_retriver.get_table_metadata(db, table).partition_columns
        return [c.name for c in cols]
    
    def repair_table(self, db: str, table: str) -> None:
        return self.partition_handler.repair_table(db, table)