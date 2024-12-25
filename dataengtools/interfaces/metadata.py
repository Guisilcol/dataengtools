from dataclasses import dataclass
from typing import List, Optional, Tuple
from abc import ABC, abstractmethod


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

@dataclass
class Partition:
    name: str
    """Partition name. Example: table_name/year=2021/month=01"""
    location: str
    """Location of the partition. Example: s3://bucket/table_name/year=2021/month=01"""
    values: Tuple[str]
    """Partition values. Example: ('2021', '01')"""
    raw_metadata: Optional[dict] = None
    """Raw metadata from the catalog. It can be used to store additional information. Example: AWS Glue metadata retrieved from Glue boto3 client"""
    
    
class TableMetadataRetriver(ABC):
    """
    Abstract base class for retrieving table metadata.
    """
    @abstractmethod
    def get_table_metadata(self, database: str, table: str) -> TableMetadata:
        """
        Retrieve metadata for a specific table in a database.

        :param database: The name of the database.
        :param table: The name of the table.
        :return: TableMetadata object containing metadata of the table.
        """
        pass

class PartitionHandler(ABC):
    """
    Abstract base class for handling table partitions.
    """
    @abstractmethod
    def get_partitions(self, database: str, table: str, conditions: Optional[str]) -> List[Partition]:
        """
        Retrieve partitions for a specific table in a database.

        :param database: The name of the database.
        :param table: The name of the table.
        :param conditions: Optional conditions to filter partitions.
        :return: List of Partition objects.
        """
        pass
    
    def delete_partitions(self, database: str, table: str, partition: List[Partition]) -> None:
        """
        Delete specified partitions from a table in a database.

        :param database: The name of the database.
        :param table: The name of the table.
        :param partition: List of Partition objects to be deleted.
        """
        pass
    
    def repair_table(self, database: str, table: str) -> None:
        """
        Repair the table in the database.

        :param database: The name of the database.
        :param table: The name of the table.
        """
        pass

