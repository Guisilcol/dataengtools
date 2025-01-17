from abc import ABC, abstractmethod
from typing import List, Optional, TypeVar, Generic
from dataengtools.core.interfaces.integration_layer.catalog_metadata import TableMetadata
from dataengtools.core.interfaces.integration_layer.catalog_partitions import Partition


Frame = TypeVar('Frame')
"""Generic type variable"""

class CatalogEngine(Generic[Frame], ABC):
    @abstractmethod
    def get_location(self, db: str, table: str) -> str:
        pass
    
    @abstractmethod
    def get_table_metadata(self, db: str, table: str) -> TableMetadata:
        pass
    
    @abstractmethod
    def get_partitions(self, db: str, table: str, conditions: Optional[str] = None) -> List[Partition]:
        pass
    
    @abstractmethod
    def read_table(self, db: str, table: str, columns: Optional[List[str]] = None) -> Frame:
        pass
    
    @abstractmethod
    def read_partitioned_table(self, db: str, table: str, conditions: str, columns: List[str] = None) -> Frame:
        pass
    
    @abstractmethod
    def adapt_frame_to_table_schema(self, df: Frame, db: str, table: str) -> Frame:
        pass
    
    @abstractmethod
    def write_table(self, df: Frame, db: str, table: str, overwrite: bool, compreesion: Optional[str] = None) -> None:
        pass
    
    @abstractmethod
    def get_partitions_columns(self, db: str, table: str) -> List[str]:
        pass

    @abstractmethod
    def repair_table(self, db: str, table: str) -> None:
        pass
    
    @abstractmethod
    def delete_partitions(self, db: str, table: str, partitions: Optional[List[Partition]] = None) -> None:
        """
        Delete partitions from a table. If partitions is None, all partitions and ONLY files in partitions will be deleted. Files that are not in partitions will not be deleted.
        """
        pass

    def truncate_table(self, db: str, table: str) -> None:
        """
        Truncate a table. If table is partitioned, all partitions and files in table location will be deleted.
        """
        pass