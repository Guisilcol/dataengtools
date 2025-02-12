from abc import ABC, abstractmethod
from typing import List, Optional, TypeVar, Generic, Generator
from dataengtools.core.interfaces.integration_layer.catalog_metadata import TableMetadata
from dataengtools.core.interfaces.integration_layer.catalog_partitions import Partition
from dataengtools.core.interfaces.io.reader import ResultSet

Frame = TypeVar("Frame")

"""Generic type variable"""

class CatalogEngine(Generic[ResultSet, Frame], ABC):
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
    def read_table(
        self, 
        db: str, 
        table: str, 
        condition: Optional[str], 
        columns: Optional[List[str]] = None
    ) -> ResultSet:
        pass
    
    @abstractmethod
    def read_table_batched(
        self, 
        db: str, 
        table: str, 
        condition: Optional[str], 
        columns: Optional[List[str]] = None, 
        order_by: Optional[List[str]] = None,
        batch_size: int = 10000
    ) -> Generator[ResultSet, None, None]:
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
        pass

    @abstractmethod
    def get_partitions_columns(self, db: str, table: str) -> List[str]:
        pass

    @abstractmethod
    def repair_table(self, db: str, table: str) -> None:
        pass

    @abstractmethod
    def delete_partitions(
        self, 
        db: str, 
        table: str, 
        partitions: Optional[List[Partition]] = None
    ) -> None:
        """
        Delete partitions from a table. If partitions is None, all partitions and ONLY files in partitions will be deleted.
        Files that are not in partitions will not be deleted.
        """
        pass

    def truncate_table(self, db: str, table: str) -> None:
        """
        Truncate a table. If table is partitioned, all partitions and files in table location will be deleted.
        """
        pass
