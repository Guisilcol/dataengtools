from typing import Generator, List, Optional, Any, overload
from dataengtools.core.interfaces.engine_layer.catalog import CatalogEngine
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.integration_layer.catalog_metadata import TableMetadata, TableMetadataRetriver 
from dataengtools.core.interfaces.integration_layer.catalog_partitions import Partition, PartitionHandler
from dataengtools.core.interfaces.io.reader import Reader

from duckdb import DuckDBPyRelation
from polars import DataFrame, from_arrow


class DuckDBCatalogEngine(CatalogEngine[DuckDBPyRelation, Any]):
    """
    Implementation of CatalogEngine using DuckDB as the backend.

    This engine provides methods for interacting with a data catalog stored in AWS Glue,
    leveraging DuckDB for SQL operations and file system integration.
    """

    def __init__(self, 
                 partition_handler: PartitionHandler, 
                 table_metadata_retriver: TableMetadataRetriver,
                 filesystem: FilesystemHandler,
                 reader: Reader[DuckDBPyRelation]
    ):
        """
        Initialize a new instance of DuckDBCatalogEngine.

        Parameters:
            partition_handler (PartitionHandler): Handler for managing table partitions.
            table_metadata_retriver (TableMetadataRetriver): Retriever for obtaining table metadata.
            filesystem (FilesystemHandler): Filesystem handler for managing file operations.
            reader (Reader[DuckDBPyRelation]): Reader instance for loading table data.
        """
        self.partition_handler = partition_handler
        self.table_metadata_retriver = table_metadata_retriver
        self.filesystem = filesystem
        self.reader = reader
        
    def get_location(self, db: str, table: str) -> str:
        """
        Retrieve the storage location of the specified table.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.

        Returns:
            str: The storage location (e.g., S3 path) of the table data, with any trailing slash removed.
        """
        location = self.table_metadata_retriver.get_table_metadata(db, table).location
        return location.rstrip("/")

    def get_table_metadata(self, db: str, table: str) -> TableMetadata:
        """
        Retrieve metadata for the specified table.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.

        Returns:
            TableMetadata: An object containing metadata information for the table.
        """
        return self.table_metadata_retriver.get_table_metadata(db, table)
    
    def get_partitions(self, db: str, table: str, conditions: Optional[str] = None) -> List[Partition]:
        """
        Retrieve partitions for the specified table within a database.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.
            conditions (Optional[str]): Optional filter conditions for selecting partitions.

        Returns:
            List[Partition]: A list of partitions matching the given conditions.
        """
        return self.partition_handler.get_partitions(db, table, conditions)
    
    def get_partitions_columns(self, db: str, table: str) -> List[str]:
        """
        Retrieve the list of column names used for partitioning the specified table.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.

        Returns:
            List[str]: A list of partition column names.
        """
        cols = self.table_metadata_retriver.get_table_metadata(db, table).partition_columns
        return [c.name for c in cols]
    
    def repair_table(self, db: str, table: str) -> None:
        """
        Repair the metadata of a table by refreshing its partition information.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.

        Returns:
            None
        """
        self.partition_handler.repair_table(db, table)
        
    def delete_partitions(self, db: str, table: str, partitions: Optional[List[Partition]] = None) -> None:
        """
        Delete specified partitions from a table and remove corresponding files.

        If no partitions are provided, this method retrieves and deletes all partitions.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.
            partitions (Optional[List[Partition]]): A list of partitions to be deleted.

        Returns:
            None
        """
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
        """
        Truncate the specified table by removing all data and partition metadata.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.

        Returns:
            None
        """
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
        """
        Read data from the specified table using provided conditions and columns selection.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.
            condition (Optional[str]): An optional SQL condition to filter the data.
            columns (Optional[List[str]]): A list of columns to retrieve; if None, all columns are returned.

        Returns:
            DuckDBPyRelation: The resulting data relation from the read operation.
        """
        metadata = self.get_table_metadata(db, table)
        data = self.reader.read(
            metadata.location,
            {
                "columns": columns,
                "file_type": metadata.files_extension,
                "separator": metadata.columns_separator,
                "has_header": metadata.files_have_header,
                "condition": condition
            }
        )
        return data
    
    def write_table(self, df, db, table, overwrite, compreesion = None):
        """
        Write a dataframe to a table in the catalog.

        This method is not implemented in DuckDBCatalogEngine. Subclasses must override
        this method to provide a concrete implementation for writing data.

        Parameters:
            df: The dataframe to write.
            db (str): The name of the database.
            table (str): The name of the table.
            overwrite (bool): Flag indicating if existing data should be overwritten.
            compreesion (Optional[Any]): Optional compression configuration.

        Raises:
            NotImplementedError: Always raised as this method is not implemented.
        """
        raise NotImplementedError("This class does not have a concrete implementation of this method")


class PolarsCatalogEngine(DuckDBCatalogEngine):
    """
    Catalog engine implementation that returns data as Polars DataFrames.

    Extends DuckDBCatalogEngine to enable data retrieval in the form of Polars DataFrames,
    supporting both single batch and batched generators.
    """

    @overload
    def read_table(self, db: str, table: str, condition: str, *, columns: Optional[List[str]] = None) -> DataFrame:
        """
        Overload signature for read_table when a condition is provided.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.
            condition (str): An SQL condition to filter data.
            columns (Optional[List[str]]): List of columns to retrieve.

        Returns:
            DataFrame: The complete table data as a Polars DataFrame.
        """
        ...

    @overload
    def read_table(self, db: str, table: str, condition: None, *, batch_size: int = 100_000, columns: Optional[List[str]] = None) -> Generator[DataFrame, None, None]:
        """
        Overload signature for read_table for batched retrieval.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.
            condition (None): No filtering condition.
            batch_size (int): The number of records per batch.
            columns (Optional[List[str]]): List of columns to retrieve.

        Returns:
            Generator[DataFrame, None, None]: A generator yielding batches of Polars DataFrames.
        """
        ...

    def read_table(
        self, 
        db: str, 
        table: str, 
        condition: Optional[str] = None, 
        *, 
        batch_size: int = 100_000, 
        columns: Optional[List[str]] = None
    ) -> Any:
        """
        Read data from the specified table and return it as one or more Polars DataFrames.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.
            condition (Optional[str]): An optional SQL condition to filter the data.
            batch_size (int): The number of records per batch; if set, returns a generator of batches.
            columns (Optional[List[str]]): List of columns to retrieve; if None, all columns are returned.

        Returns:
            DataFrame or Generator[DataFrame, None, None]:
                - A single Polars DataFrame if batch_size is not used.
                - A generator yielding Polars DataFrame batches if batch_size is specified.
        """
        data = super().read_table(db, table, condition, columns)

        if batch_size:
            for batch in data.record_batch(batch_size):
                yield from_arrow(batch)
        else:
            return data.pl()