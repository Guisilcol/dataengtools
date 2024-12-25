from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional

K = TypeVar('K')
V = TypeVar('V')

class DataTypeMapping(ABC, Generic[K, V]):
    MAPPING = {}
    
    def get(self, key: K, default: Optional[V] = None) -> V:
        if not self.MAPPING:
            raise NotImplementedError("DataType Mapping not defined")
        
        return self.MAPPING.get(key, default)

    