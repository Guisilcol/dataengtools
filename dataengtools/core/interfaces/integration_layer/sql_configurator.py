from abc import ABC, abstractmethod
from typing import TypeVar, Generic

Connection = TypeVar('Connection')
"""Generic type variable"""

class SQLProviderConfigurator(ABC, Generic[Connection]):
    @abstractmethod
    def configure_connection(self, connection: Connection) -> Connection:
        pass
