from dataclasses import dataclass
from typing import List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod

K = TypeVar('K')
V = TypeVar('V')

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
    """Source of the table. Example: Glue Catalog, Athena, Redshift, etc"""

 
class TableMetadataRetriver(ABC):
    """
    Abstract base class for retrieving table metadata.
    """
    @abstractmethod
    def get_table_metadata(self, database: str, table: str, additional_configs: dict = {}) -> TableMetadata:
        """
        Retrieve metadata for a specific table in a database.

        :param database: The name of the database.
        :param table: The name of the table.
        :return: TableMetadata object containing metadata of the table.
        """
        pass

class DataTypeMapping(ABC, Generic[K, V]):
    MAPPING = {}
    
    def get(self, key: K, default: Optional[V] = None) -> V:
        if not self.MAPPING:
            raise NotImplementedError("DataType Mapping not defined")
        
        return self.MAPPING.get(key, default)
