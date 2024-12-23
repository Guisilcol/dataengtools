from abc import ABC, abstractmethod
from typing import List

class FilesystemOperationsHandler(ABC):
    """
    Abstract base class for handling filesystem operations.
    """
    @abstractmethod
    def get_files(self, root: str, prefix: str) -> List[str]:
        """
        Retrieve a list of files from the filesystem.

        :param root: The root directory to search.
        :param prefix: The prefix to filter files.
        :return: List of file paths.
        """
        pass

    def delete_files(self, root: str, files: List[str]) -> None:
        """
        Delete specified files from the filesystem.

        :param root: The root directory.
        :param files: List of file paths to be deleted.
        """
        pass