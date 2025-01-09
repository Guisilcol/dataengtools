from abc import ABC, abstractmethod
from typing import List, Optional, TypeVar, Generic
from dataengtools.core.interfaces.integration_layer.catalog_metadata import TableMetadata
from dataengtools.core.interfaces.integration_layer.catalog_partitions import Partition


T = TypeVar('T')
"""Generic type variable"""

class Catalog(Generic[T], ABC):
    @abstractmethod
    def get_location(self, db: str, table: str) -> str:
        pass
    
    @abstractmethod
    def get_table_metadata(self, db: str, table: str) -> TableMetadata:
        pass
    
    @abstractmethod
    def get_partitions(self, db: str, table: str, conditions: Optional[str] = None) -> List[str]:
        pass
    
    @abstractmethod
    def read_table(self, db: str, table: str, columns: Optional[List[str]] = None) -> T:
        pass
    
    @abstractmethod
    def read_partitioned_table(self, db: str, table: str, conditions: str, columns: List[str] = None) -> T:
        pass
    
    @abstractmethod
    def adapt_frame_to_table_schema(self, df: T, db: str, table: str) -> T:
        pass
    
    @abstractmethod
    def write_table(self, df: T, db: str, table: str, overwrite: bool, compreesion: Optional[str] = None) -> None:
        pass
    
    @abstractmethod
    def get_partitions_columns(self, db: str, table: str) -> List[str]:
        pass

    @abstractmethod
    def repair_table(self, db: str, table: str) -> None:
        pass
    
    @abstractmethod
    def delete_partitions(self, db: str, table: str, partitions: List[str]) -> None:
        pass
