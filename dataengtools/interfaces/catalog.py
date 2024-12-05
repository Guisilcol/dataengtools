from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List

T = TypeVar('T')

class Catalog(Generic[T], ABC):
    @abstractmethod
    def get_location(self) -> str:
        pass
    
    @abstractmethod
    def read_table(self, db: str, table: str) -> T:
        pass
    
    @abstractmethod
    def read_partitioned_table(self, db: str, table: str, conditions: str) -> T:
        pass
    
    @abstractmethod
    def adapt_frame_to_table_schema(self, df: T, db: str, table: str) -> T:
        pass
    
    @abstractmethod
    def write_table(self, df: T, db: str, table: str, overwrite: bool) -> None:
        pass
    
    @abstractmethod
    def get_partition_columns(self, db: str, table: str) -> List[str]:
        pass

