from abc import ABC, abstractmethod
from io import TextIOWrapper
from typing import List

class FilesystemHandler(ABC):
    """
    Abstract base class for handling filesystem operations.
    """
    @abstractmethod
    def get_filepaths(self, root: str, prefix: str) -> List[str]:
        """
        Retrieve a list of files from the filesystem.

        :param root: The root directory to search.
        :param prefix: The prefix to filter files.
        :return: List of file paths.
        """
        pass

    @abstractmethod
    def delete_files(self, root: str, files: List[str]) -> None:
        """
        Delete specified files from the filesystem.

        :param root: The root directory.
        :param files: List of file paths to be deleted.
        """
        pass
    
    @abstractmethod
    def open_file(self, path: str, mode: str) -> TextIOWrapper:
        """
        Open a file in the filesystem.

        :param path: The file path.
        :param mode: The mode to open the file.
        """
        pass