from abc import ABC, abstractmethod
from typing import List, Optional, TypeVar, Generic, Generator
from dataengtools.core.interfaces.integration_layer.catalog_metadata import TableMetadata
from dataengtools.core.interfaces.integration_layer.catalog_partitions import Partition
from dataengtools.core.interfaces.io.reader import ResultSet

Frame = TypeVar("Frame")
"""Generic type variable for representing a dataframe or similar structure"""

class CatalogEngine(Generic[ResultSet, Frame], ABC):
    """
    Abstract base class for catalog engine implementations.

    CatalogEngine defines the interface for operations on a data catalog,
    such as retrieving table locations, metadata, partitions, and performing table
    read/write operations.
    """

    @abstractmethod
    def get_location(self, db: str, table: str) -> str:
        """
        Retrieve the storage location of a table.

        Parameters:
            db (str): The database name.
            table (str): The table name.

        Returns:
            str: The location (e.g., S3 path) where the table data is stored.
        """
        pass

    @abstractmethod
    def get_table_metadata(self, db: str, table: str) -> TableMetadata:
        """
        Fetch metadata details for a specified table.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.

        Returns:
            TableMetadata: An object containing metadata information about the table.
        """
        pass

    @abstractmethod
    def get_partitions(self, db: str, table: str, conditions: Optional[str] = None) -> List[Partition]:
        """
        Retrieve the list of partitions for a table.

        Parameters:
            db (str): The name of the database.
            table (str): The name of the table.
            conditions (Optional[str]): Optional conditions to filter partitions.

        Returns:
            List[Partition]: A list of partitions that match the provided conditions.
        """
        pass

    @abstractmethod
    def read_table(
        self, 
        db: str, 
        table: str, 
        condition: Optional[str], 
        columns: Optional[List[str]] = None
    ) -> ResultSet:
        """
        Read data from a table based on the provided condition and selected columns.

        Parameters:
            db (str): The database name.
            table (str): The table name.
            condition (Optional[str]): SQL-like condition to filter data.
            columns (Optional[List[str]]): List of columns to retrieve. If None, all columns are retrieved.

        Returns:
            ResultSet: The result set containing the table data.
        """
        pass
    
    @abstractmethod
    def write_table(
        self, 
        df: Frame, 
        db: str, 
        table: str, 
        overwrite: bool, 
        compreesion: Optional[str] = None
    ) -> None:
        """
        Write a dataframe to a table in the catalog.

        Parameters:
            df (Frame): The dataframe to be written.
            db (str): The target database name.
            table (str): The target table name.
            overwrite (bool): Flag indicating whether to overwrite existing data.
            compreesion (Optional[str]): Optional compression format to use when writing data.

        Returns:
            None
        """
        pass

    @abstractmethod
    def get_partitions_columns(self, db: str, table: str) -> List[str]:
        """
        Retrieve the list of columns used for partitioning a table.

        Parameters:
            db (str): The database name.
            table (str): The table name.

        Returns:
            List[str]: A list of column names that are used as partition keys.
        """
        pass

    @abstractmethod
    def repair_table(self, db: str, table: str) -> None:
        """
        Repair a table's metadata by refreshing its partition information.

        Parameters:
            db (str): The database name.
            table (str): The table name.

        Returns:
            None
        """
        pass

    @abstractmethod
    def delete_partitions(
        self, 
        db: str, 
        table: str, 
        partitions: Optional[List[Partition]] = None
    ) -> None:
        """
        Delete one or more partitions from a table.

        If no partitions are specified (i.e., partitions is None), then all partitions
        and the files within those partitions will be deleted. Files outside of the
        partitioned directories will remain untouched.

        Parameters:
            db (str): The database name.
            table (str): The table name.
            partitions (Optional[List[Partition]]): A list of partitions to delete. If None, all partitions are deleted.

        Returns:
            None
        """
        pass

    def truncate_table(self, db: str, table: str) -> None:
        """
        Truncate the entire table by deleting all data and partitions.

        For partitioned tables, this operation will remove all partitions and delete all files
        in the table's storage location.

        Parameters:
            db (str): The database name.
            table (str): The table name.

        Returns:
            None
        """
        pass