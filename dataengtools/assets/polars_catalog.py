from typing import List, Optional, TypeVar
import polars as pl
from uuid import uuid4

from dataengtools.assets.template_catalog import CatalogTemplate
from dataengtools.interfaces.metadata import PartitionHandler, TableMetadataRetriver
from dataengtools.interfaces.filesystem import FilesystemHandler
from dataengtools.interfaces.datatype_mapping import DataTypeMapping
from dataengtools.logger import Logger

LOGGER = Logger.get_instance()

T = TypeVar('T')


class PolarsDataFrameCatalog(CatalogTemplate[pl.DataFrame]):
    
    def __init__(self, 
                 partition_handler: PartitionHandler, 
                 table_metadata_retriver: TableMetadataRetriver,
                 filesystem: FilesystemHandler,
                 datatype_mapping: DataTypeMapping[str, pl.DataType]
    ):
        super().__init__(partition_handler, table_metadata_retriver, filesystem)
        self.datatype_mapping = datatype_mapping
    
    def read_table(self, db: str, table: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        
        filepath = metadata.location + '/**'
        LOGGER.info(f'Reading table "{db}.{table}" from "{filepath}"')
        
        if metadata.files_extension == 'parquet':
            return pl.read_parquet(filepath, columns=columns)
        
        if metadata.files_extension == 'csv':
            return pl.read_csv(
                filepath, 
                separator=metadata.columns_separator,
                has_header=metadata.files_have_header,
                columns=columns
            )

        raise ValueError(f'Unsupported files extension "{metadata.files_extension}"')
    
    def read_partitioned_table(self, db: str, table: str, conditions: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        
        if not metadata.partition_columns:
            raise ValueError(f'Table "{db}.{table}" is not partitioned')
        
        partitions = self.partition_handler.get_partitions(db, table, conditions)

        LOGGER.info(f'Reading partitioned table "{db}.{table}" with conditions "{conditions}"')
        LOGGER.info(f'Partitions: {[p.name for p in partitions]}')
        
        if metadata.files_extension == 'parquet':
            dfs = []
            for partition in partitions:
                lf = pl.read_parquet(partition.location + '/**', columns=columns)
                dfs.append(lf)

        elif metadata.files_extension == 'csv':
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
        
        else:
            raise ValueError(f'Unsupported files extension {metadata.files_extension}')
        
        if not dfs:
            return pl.DataFrame([])
        
        return pl.concat(dfs)
        
    def adapt_frame_to_table_schema(self, df: pl.DataFrame, db: str, table: str) -> pl.DataFrame:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        
        # Create columns that are missing in the DataFrame with None values
        for column in metadata.all_columns:
            if column.name not in df.columns:
                df = (
                    df
                    .with_columns(
                        pl.lit(None)
                        .cast(self.datatype_mapping.get(column.datatype, pl.Object))
                        .alias(column.name)
                    )
                )

        # Remove columns that are not in the table schema
        df = df.select(*[c.name for c in metadata.all_columns])
        
        # Cast columns to the correct datatype
        df = df.with_columns([
            pl.col(column.name)
                .cast(self.datatype_mapping.get(column.datatype, pl.Object)) 
            for column in metadata.columns
        ])
        
        LOGGER.info(f'Adapted DataFrame to table "{db}.{table}" schema: {df.schema}')

        return df
        
    def write_table(self, df: pl.DataFrame, db: str, table: str, overwrite: bool, compreesion: str = 'snappy') -> None:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        filename = str(uuid4()) + '.' + metadata.files_extension
        
        if overwrite:
            LOGGER.info(f'Truncating table "{db}.{table}"')
            self.delete_partitions(db, table, self.get_partitions(db, table, None))
            
        # If the table is not partitioned, write the DataFrame as a single file    
        if not metadata.partition_columns:            
            filepath = location + '/' + filename    
            with self.filesystem.open_file(filepath, 'wb') as f:
                LOGGER.info(f'Writing table "{db}.{table}" to "{filepath}"')
                if metadata.files_extension == 'parquet':
                    df.write_parquet(f, compression=compreesion)
                    return
                    
                if metadata.files_extension == 'csv':
                    df.write_csv(f, 
                                include_header=metadata.files_have_header, 
                                separator=metadata.columns_separator)
                    return
                
                raise ValueError(f'Unsupported files extension {metadata.files_extension}')
            
        partition_columns = metadata.partition_columns
        
        for (partition_values, grouped_df) in df.group_by(*[p.name for p in partition_columns]):
            partition_name = '/'.join([f'{p.name}={v}' for p, v in zip(partition_columns, partition_values)])
            location = self.get_location(db, table) + '/' + partition_name
            filepath = location + '/' + filename
            
            with self.filesystem.open_file(filepath, 'wb') as f:
                LOGGER.info(f'Writing partition "{partition_name}" to "{filepath}"')
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
