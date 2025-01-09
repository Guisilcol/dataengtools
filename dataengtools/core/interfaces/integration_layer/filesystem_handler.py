from abc import ABC, abstractmethod
from io import TextIOWrapper
from typing import List

class FilesystemHandler(ABC):
    """
    Abstract base class for handling filesystem operations.
    """
    @abstractmethod
    def get_files(self, prefix: str, additional_configs: dict = {}) -> List[str]:
        pass

    @abstractmethod
    def delete_files(self, files: List[str], additional_configs: dict = {}) -> None:
        pass
    
    @abstractmethod
    def open_file(self, path: str, mode: str, additional_configs: dict = {}) -> TextIOWrapper:
        pass
    