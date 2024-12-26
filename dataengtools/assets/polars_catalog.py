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
    """
    A catalog implementation for managing Polars DataFrame operations. This class provides
    functionalities to read, write, and adapt data to match a specific table schema.
    """

    def __init__(self, 
                 partition_handler: PartitionHandler, 
                 table_metadata_retriver: TableMetadataRetriver,
                 filesystem: FilesystemHandler,
                 datatype_mapping: DataTypeMapping[str, pl.DataType]):
        """
        Initialize the PolarsDataFrameCatalog.

        Args:
            partition_handler (PartitionHandler): Handles partition-related operations.
            table_metadata_retriver (TableMetadataRetriver): Retrieves table metadata.
            filesystem (FilesystemHandler): Handles filesystem interactions.
            datatype_mapping (DataTypeMapping[str, pl.DataType]): Maps datatypes between schemas and Polars DataFrame.
        """
        super().__init__(partition_handler, table_metadata_retriver, filesystem)
        self.datatype_mapping = datatype_mapping

    def _read_file(self, filepath: str, file_extension: str, metadata, columns: Optional[List[str]]) -> pl.DataFrame:
        """
        Internal method to read a file based on its extension.

        Args:
            filepath (str): Path to the file.
            file_extension (str): File extension (e.g., 'parquet', 'csv').
            metadata: Metadata associated with the table.
            columns (Optional[List[str]]): Specific columns to read.

        Returns:
            pl.DataFrame: The DataFrame read from the file.

        Raises:
            ValueError: If the file extension is unsupported.
        """
        if file_extension == 'parquet':
            return pl.read_parquet(filepath, columns=columns)
        elif file_extension == 'csv':
            return pl.read_csv(
                filepath,
                separator=metadata.columns_separator,
                has_header=metadata.files_have_header,
                columns=columns
            )
        else:
            raise ValueError(f'Unsupported file extension "{file_extension}"')

    def read_table(self, db: str, table: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Read a table into a Polars DataFrame.

        Args:
            db (str): Database name.
            table (str): Table name.
            columns (Optional[List[str]]): Columns to include in the result.

        Returns:
            pl.DataFrame: The resulting DataFrame.
        """
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        filepath = f"{metadata.location}/**"
        LOGGER.debug(f'Reading table "{db}.{table}" from "{filepath}"')
        return self._read_file(filepath, metadata.files_extension, metadata, columns)

    def read_partitioned_table(self, db: str, table: str, conditions: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Read a partitioned table into a Polars DataFrame based on specified conditions.

        Args:
            db (str): Database name.
            table (str): Table name.
            conditions (str): Conditions to filter partitions.
            columns (Optional[List[str]]): Columns to include in the result.

        Returns:
            pl.DataFrame: The resulting DataFrame.

        Raises:
            ValueError: If the table is not partitioned or if the file extension is unsupported.
        """
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)

        if not metadata.partition_columns:
            raise ValueError(f'Table "{db}.{table}" is not partitioned')

        partitions = self.partition_handler.get_partitions(db, table, conditions)
        LOGGER.debug(f'Reading partitioned table "{db}.{table}" with conditions "{conditions}"')
        LOGGER.debug(f'Partitions: {[p.name for p in partitions]}')

        dfs = [
            self._read_file(partition.location + '/**', metadata.files_extension, metadata, columns)
            for partition in partitions
        ]

        return pl.concat(dfs) if dfs else pl.DataFrame([])

    def adapt_frame_to_table_schema(self, df: pl.DataFrame, db: str, table: str) -> pl.DataFrame:
        """
        Adapt a DataFrame to match the schema of a specified table.

        Args:
            df (pl.DataFrame): The input DataFrame.
            db (str): Database name.
            table (str): Table name.

        Returns:
            pl.DataFrame: The adapted DataFrame.
        """
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        column_names = [col.name for col in metadata.all_columns]

        # Add missing columns
        missing_columns = [
            pl.lit(None).cast(self.datatype_mapping.get(col.datatype, pl.Object)).alias(col.name)
            for col in metadata.all_columns if col.name not in df.columns
        ]
        if missing_columns:
            df = df.with_columns(missing_columns)

        # Select relevant columns and cast them to the correct datatype
        df = df.select(column_names).with_columns([
            pl.col(col.name).cast(self.datatype_mapping.get(col.datatype, pl.Object))
            for col in metadata.columns
        ])

        LOGGER.debug(f'Adapted DataFrame to table "{db}.{table}" schema: {df.schema}')
        return df

    def _write_file(self, df: pl.DataFrame, filepath: str, file_extension: str, metadata, compression: str):
        """
        Internal method to write a DataFrame to a file.

        Args:
            df (pl.DataFrame): The DataFrame to write.
            filepath (str): Path to the file.
            file_extension (str): File extension (e.g., 'parquet', 'csv').
            metadata: Metadata associated with the table.
            compression (str): Compression type to use.

        Raises:
            ValueError: If the file extension is unsupported.
        """
        if file_extension == 'parquet':
            df.write_parquet(filepath, compression=compression)
        elif file_extension == 'csv':
            df.write_csv(
                filepath,
                include_header=metadata.files_have_header,
                separator=metadata.columns_separator
            )
        else:
            raise ValueError(f'Unsupported file extension "{file_extension}"')

    def write_table(self, df: pl.DataFrame, db: str, table: str, overwrite: bool, compression: str = 'snappy') -> None:
        """
        Write a DataFrame to a table, optionally overwriting existing data.

        Args:
            df (pl.DataFrame): The DataFrame to write.
            db (str): Database name.
            table (str): Table name.
            overwrite (bool): Whether to overwrite existing data.
            compression (str): Compression type to use (default: 'snappy').

        Raises:
            ValueError: If the file extension is unsupported.
        """
        metadata = self.table_metadata_retriver.get_table_metadata(db, table)
        filename = f"{uuid4()}.{metadata.files_extension}"

        if overwrite:
            LOGGER.debug(f'Truncating table "{db}.{table}"')
            self.delete_partitions(db, table, self.get_partitions(db, table, None))

        if not metadata.partition_columns:
            filepath = f"{metadata.location}/{filename}"
            LOGGER.debug(f'Writing table "{db}.{table}" to "{filepath}"')
            self._write_file(df, filepath, metadata.files_extension, metadata, compression)
            return

        for partition_values, grouped_df in df.group_by(*[col.name for col in metadata.partition_columns]):
            partition_name = '/'.join(f"{col.name}={val}" for col, val in zip(metadata.partition_columns, partition_values))
            location = f"{self.get_location(db, table)}/{partition_name}"
            filepath = f"{location}/{filename}"
            LOGGER.debug(f'Writing partition "{partition_name}" to "{filepath}"')
            self._write_file(grouped_df, filepath, metadata.files_extension, metadata, compression)

        self.repair_table(db, table)
