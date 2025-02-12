from abc import ABC, abstractmethod
from typing import Optional, List, TypeVar, Generic, Tuple

ResultSet = TypeVar('ResultSet')

class Reader(Generic[ResultSet], ABC):

    @abstractmethod
    def read(self, 
             path: str, 
             have_header: Optional[bool] = None,
             delimiter: Optional[str] = None,
             file_type: Optional[str] = None,
             columns: Optional[List[str]] = None, 
             condition: Optional[str] = None,
             order_by: Optional[List[str]] = None, 
             offset: Optional[int] = None,
             limit: Optional[int] = None
    ) -> Tuple[ResultSet, int]:
        pass