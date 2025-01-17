from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional

Connection = TypeVar('Connection')
ResultSet = TypeVar('ResultSet')
"""Generic type variable"""

class SQLProviderConfigurator(ABC, Generic[Connection]):
    @abstractmethod
    def configure_connection(self, connection: Connection) -> Connection:
        pass

class SQLEngine(ABC, Generic[Connection, ResultSet]):

    @abstractmethod
    def get_connection(self) -> Connection:
        pass

    @abstractmethod
    def execute(self, query: str, params: dict = {}) -> None:
        pass

    @abstractmethod
    def execute_and_fetch(self, query: str, params: dict = {}) -> ResultSet:
        pass
