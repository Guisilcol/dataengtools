from dataclasses import dataclass
from typing import List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod

#####################
# Database Metadata #
#####################

@dataclass
class DatabaseMetadata:
    name: str
    """Database name"""
    tables: List[str]
    """List of tables"""
    raw_metadata: Optional[dict] = None
    """Raw metadata from the catalog. It can be used to store additional information. Example: AWS Glue metadata retrieved from Glue boto3 client"""
    source: Optional[str] = None
    """Source of the database. Example: Glue CatalogEngine, Athena, Redshift, etc"""

class DatabaseMetadataRetriever(ABC):
    """
    Abstract base class for retrieving database metadata.
    """

    @abstractmethod
    def get_all_databases(self, additional_configs: dict = {}) -> List[DatabaseMetadata]:
        """
        Retrieve a list of all databases.

        :return: List of database names.
        """
        pass

    @abstractmethod
    def get_database_metadata(self, database: str, additional_configs: dict = {}) -> DatabaseMetadata:
        """
        Retrieve metadata for a specific database.

        :param database: The name of the database.
        :return: DatabaseMetadata object containing metadata of the database.
        """
        pass

##################
# Table Metadata #
##################

@dataclass
class Column:
    name: str
    """It is the name of the column"""
    datatype: str
    """It is a string representation of the type"""

@dataclass
class TableMetadata:
    database: str
    """Database name"""
    table: str
    """Table name"""
    columns: List[Column]
    """List of columns"""
    partition_columns: List[Column]
    """List of partition columns"""
    all_columns: List[Column]
    """List of all columns (columns + partition_columns)"""
    location: str
    """Location of the table. """
    files_have_header: bool
    """If the files have header"""
    files_extension: str
    """Extension of the files. Example: parquet, csv"""
    columns_separator: str
    """Separator of the columns. Example: ;, |, \t"""
    raw_metadata: Optional[dict] = None
    """Raw metadata from the catalog. It can be used to store additional information. Example: AWS Glue metadata retrieved from Glue boto3 client"""
    source: Optional[str] = None
    """Source of the table. Example: Glue CatalogEngine, Athena, Redshift, etc"""

class TableMetadataRetriver(ABC):
    """
    Abstract base class for retrieving table metadata.
    """

    @abstractmethod
    def get_all_tables(self, database: str, additional_configs: dict = {}) -> List[TableMetadata]:
        """
        Retrieve a list of all tables in a database.

        :param database: The name of the database.
        :return: List of TableMetadata objects containing metadata of the tables.
        """
        pass

    @abstractmethod
    def get_table_metadata(self, database: str, table: str, additional_configs: dict = {}) -> TableMetadata:
        """
        Retrieve metadata for a specific table in a database.

        :param database: The name of the database.
        :param table: The name of the table.
        :return: TableMetadata object containing metadata of the table.
        """
        pass

#####################
# Data Type Mapping #
#####################

K = TypeVar('K')
V = TypeVar('V')

class DataTypeMapping(ABC, Generic[K, V]):
    MAPPING = {}
    
    def get(self, key: K, default: Optional[V] = None) -> V:
        if not self.MAPPING:
            raise NotImplementedError("DataType Mapping not defined")
        
        return self.MAPPING.get(key, default)
