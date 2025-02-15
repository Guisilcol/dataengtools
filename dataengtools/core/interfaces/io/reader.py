from abc import ABC, abstractmethod
from typing import Optional, List, TypeVar, Generic, Tuple, TypedDict

ResultSet = TypeVar('ResultSet')

class ReaderOptions(TypedDict, total=False):
    """Dictionary type for reader metadata."""
    separator: Optional[str]
    file_type: Optional[str]
    columns: Optional[List[str]]
    condition: Optional[str]
    order_by: Optional[List[str]]
    offset: Optional[int]
    limit: Optional[int]
    has_header: Optional[bool]
    skip_rows: Optional[int]
    n_rows: Optional[int]
    encoding: Optional[str]
    hive_partitioning: Optional[bool]


class Reader(Generic[ResultSet], ABC):

    @abstractmethod
    def read(self, path: str, metadata: ReaderOptions = {}) -> ResultSet:
        pass

