from typing import List, Optional, TypeVar
import polars as pl
from uuid import uuid4

from dataengtools.engines.polars.base_catalog import CatalogTemplate
from dataengtools.core.interfaces.integration_layer.catalog_metadata import TableMetadataRetriver, DataTypeMapping
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.integration_layer.catalog_partitions import PartitionHandler
from dataengtools.utils.logger import Logger

T = TypeVar('T')

class PolarsLazyFrameCatalog(CatalogTemplate[pl.LazyFrame]):
    logger = Logger.get_instance()
    
    def __init__(self, 
                 partition_handler: PartitionHandler, 
                 table_metadata_retriver: TableMetadataRetriver,
                 filesystem: FilesystemHandler,
                 datatype_mapping: DataTypeMapping[str, pl.DataType]
    ):
        super().__init__(partition_handler, table_metadata_retriver, filesystem)
        self.datatype_mapping = datatype_mapping
    
    def read_table(self, db: str, table: str, columns: Optional[List[str]] = None) -> pl.LazyFrame:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        
        filepath = metadata.location + '/**/*' + '.' + metadata.files_extension

        self.logger.debug(f'Reading table "{db}.{table}" from "{filepath}"')
        
        if metadata.files_extension == 'parquet':
            lf = pl.scan_parquet(filepath)
            if columns:
                return lf.select(*columns)
            
            return lf
        
        if metadata.files_extension == 'csv':
            return pl.scan_csv(
                filepath,
                separator=metadata.columns_separator,
                has_header=metadata.files_have_header,
                columns=columns
            )

        raise ValueError(f'Unsupported files extension "{metadata.files_extension}"')
    
    def read_partitioned_table(self, db: str, table: str, conditions: str, columns: Optional[List[str]] = None) -> pl.LazyFrame:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)

        table_location = self.get_location(db, table)
        
        if not metadata.partition_columns:
            raise ValueError(f'Table "{db}.{table}" is not partitioned')
        
        partitions = self.partition_handler.get_partitions(db, table, conditions)

        self.logger.debug(f'Partitions captured in "{db}.{table}" table with conditions "{conditions}": {partitions}')
        
        if metadata.files_extension == 'parquet':
            lfs = []
            for partition in partitions:
                lf = pl.scan_parquet(
                    table_location + '/' + partition + '/**/*' + '.' + metadata.files_extension
                )

                if columns:
                    lf = lf.select(*columns)

                lfs.append(lf)

        elif metadata.files_extension == 'csv':
            lfs = []
            for partition in partitions:
                lf = pl.scan_csv(
                    table_location + '/' + partition + '/**/*' + '.' + metadata.files_extension, 
                    separator=metadata.columns_separator,
                    has_header=metadata.files_have_header,
                    columns=columns
                )
                lfs.append(lf)
        
        else:
            raise ValueError(f'Unsupported files extension {metadata.files_extension}')
        
        if not lfs:
            return pl.DataFrame([])
        
        return pl.concat(lfs)
        
    def adapt_frame_to_table_schema(self, lf: pl.LazyFrame, db: str, table: str) -> pl.LazyFrame:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        
        # Create columns that are missing in the DataFrame with None values
        lf_cols = lf.collect_schema().names()
        for column in metadata.all_columns:
            if column.name not in lf_cols:
                lf = (
                    lf
                    .with_columns(
                        pl.lit(None)
                        .cast(self.datatype_mapping.get(column.datatype, pl.Object))
                        .alias(column.name)
                    )
                )

        # Remove columns that are not in the table schema
        lf = lf.select(*[c.name for c in metadata.all_columns])
        
        # Cast columns to the correct datatype
        lf = lf.with_columns(*[
            pl.col(column.name)
                .cast(self.datatype_mapping.get(column.datatype, pl.Object)) 
            for column in metadata.all_columns
        ])
        
        return lf
        
    def write_table(self, lf: pl.LazyFrame, db: str, table: str, overwrite: bool, compreesion: str = 'snappy') -> None:
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        filename = str(uuid4()) + '.' + metadata.files_extension
        
        if overwrite:
            self.logger.debug(f'Truncating table "{db}.{table}"')
            self.delete_partitions(db, table, self.get_partitions(db, table, None))

        ###############################
        # Non-partitioned table write #
        ###############################

        # If the table is not partitioned, write the DataFrame as a single file    
        if not metadata.partition_columns:            
            filepath = metadata.location + '/' + filename    
            with self.filesystem.open_file(filepath, 'wb') as f:
                self.logger.debug(f'Writing table "{db}.{table}" to "{filepath}"')
                if metadata.files_extension == 'parquet':
                    lf.sink_parquet(f, compression=compreesion)
                    return
                    
                if metadata.files_extension == 'csv':
                    lf.sink_csv(f, 
                                include_header=metadata.files_have_header, 
                                separator=metadata.columns_separator)
                    return
                
                raise ValueError(f'Unsupported files extension {metadata.files_extension}')
        
        ###########################
        # Partitioned table write #
        ###########################

        partition_columns = metadata.partition_columns
        for (partition_values, grouped_df) in lf.collect().group_by(*[p.name for p in partition_columns]):
            partition_name = '/'.join([f'{p.name}={v}' for p, v in zip(partition_columns, partition_values)])
            location = self.get_location(db, table) + '/' + partition_name
            filepath = location + '/' + filename
            
            with self.filesystem.open_file(filepath, 'wb') as f:
                self.logger.debug(f'Writing partition "{partition_name}" to "{filepath}"')
                
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
