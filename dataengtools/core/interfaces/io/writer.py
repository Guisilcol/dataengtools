from abc import ABC, abstractmethod
from typing import Optional, List, TypeVar, Generic, TypedDict

ResultSet = TypeVar('ResultSet')

class WriterOptions(TypedDict, total=False):
    """Dictionary type for reader metadata."""
    separator: Optional[str]
    file_type: Optional[str]
    columns: Optional[List[str]]
    order_by: Optional[List[str]]
    has_header: Optional[bool]
    compression: Optional[str]
    partition_by: Optional[List[str]]
    file_type: Optional[str]
    mode: Optional[str]

class Writer(Generic[ResultSet], ABC):

    @abstractmethod
    def write(self, data: ResultSet, path: str, writer_options: WriterOptions = {}) -> None:
        pass

