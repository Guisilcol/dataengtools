from dataclasses import dataclass
from typing import List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod

#####################
# Database Metadata #
#####################

@dataclass
class DatabaseMetadata:
    """
    Represents metadata for a database.

    Attributes:
        name (str): The name of the database.
        tables (List[str]): A list of table names contained in the database.
        raw_metadata (Optional[dict]): Raw metadata from the catalog, which can store additional
            information (e.g., AWS Glue metadata retrieved from boto3).
        source (Optional[str]): The source of the database (e.g., Glue CatalogEngine, Athena, Redshift).
    """
    name: str
    tables: List[str]
    raw_metadata: Optional[dict] = None
    source: Optional[str] = None

class DatabaseMetadataRetriever(ABC):
    """
    Abstract base class for retrieving metadata about databases.
    """

    @abstractmethod
    def get_all_databases(self, additional_configs: dict = {}) -> List[DatabaseMetadata]:
        """
        Retrieve a list of all databases available in the catalog.

        Parameters:
            additional_configs (dict, optional): Additional configuration options (default is empty dict).

        Returns:
            List[DatabaseMetadata]: A list of DatabaseMetadata objects representing all databases.
        """
        pass

    @abstractmethod
    def get_database_metadata(self, database: str, additional_configs: dict = {}) -> DatabaseMetadata:
        """
        Retrieve metadata for a specific database.

        Parameters:
            database (str): The name of the database.
            additional_configs (dict, optional): Additional configuration options (default is empty dict).

        Returns:
            DatabaseMetadata: An object containing metadata about the specified database.
        """
        pass

##################
# Table Metadata #
##################

@dataclass
class Column:
    """
    Represents a column in a table schema.

    Attributes:
        name (str): The column's name.
        datatype (str): A string representing the column's data type.
    """
    name: str
    datatype: str

@dataclass
class TableMetadata:
    """
    Represents metadata for a table.

    Attributes:
        database (str): The name of the database containing the table.
        table (str): The name of the table.
        columns (List[Column]): A list of columns in the table.
        partition_columns (List[Column]): A list of columns used for partitioning the table.
        all_columns (List[Column]): A combined list of all columns (including partition columns).
        location (str): The storage location of the table data.
        files_have_header (bool): Indicates if the source files include a header row.
        files_extension (str): The file extension (e.g., parquet, csv).
        columns_separator (str): The delimiter used between columns (e.g., ';', '|', '\t').
        raw_metadata (Optional[dict]): Raw metadata from the catalog which may include additional details.
        source (Optional[str]): The source of the table metadata (e.g., Glue CatalogEngine, Athena).
    """
    database: str
    table: str
    columns: List[Column]
    partition_columns: List[Column]
    all_columns: List[Column]
    location: str
    files_have_header: bool
    files_extension: str
    columns_separator: str
    raw_metadata: Optional[dict] = None
    source: Optional[str] = None

class TableMetadataRetriver(ABC):
    """
    Abstract base class for retrieving metadata about tables in a database.
    """

    @abstractmethod
    def get_all_tables(self, database: str, additional_configs: dict = {}) -> List[TableMetadata]:
        """
        Retrieve metadata for all tables within a specified database.

        Parameters:
            database (str): The name of the database.
            additional_configs (dict, optional): Additional configuration options (default is empty dict).

        Returns:
            List[TableMetadata]: A list of TableMetadata objects, one for each table in the database.
        """
        pass

    @abstractmethod
    def get_table_metadata(self, database: str, table: str, additional_configs: dict = {}) -> TableMetadata:
        """
        Retrieve metadata for a specific table within a specified database.

        Parameters:
            database (str): The name of the database.
            table (str): The name of the table.
            additional_configs (dict, optional): Additional configuration options (default is empty dict).

        Returns:
            TableMetadata: An object containing metadata information for the specified table.
        """
        pass

#####################
# Data Type Mapping #
#####################

K = TypeVar('K')
V = TypeVar('V')

class DataTypeMapping(ABC, Generic[K, V]):
    """
    Abstract base class for mapping data types between different systems.

    Attributes:
        MAPPING (dict): A dictionary that defines the mapping from one data type to another.
    """
    MAPPING = {}
    
    def get(self, key: K, default: Optional[V] = None) -> V:
        """
        Retrieve the mapped value for a given key.

        Parameters:
            key (K): The key to look up in the mapping.
            default (Optional[V], optional): The default value to return if the key is not found.
                Defaults to None.

        Returns:
            V: The mapped value corresponding to the key, or the default if the key is not found.

        Raises:
            NotImplementedError: If the MAPPING dictionary has not been defined.
        """
        if not self.MAPPING:
            raise NotImplementedError("DataType Mapping not defined")
        
        return self.MAPPING.get(key, default)