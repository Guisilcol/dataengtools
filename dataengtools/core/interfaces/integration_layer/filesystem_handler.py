from abc import ABC, abstractmethod
from io import TextIOWrapper
from typing import List

class FilesystemHandler(ABC):
    """
    Abstract base class for handling filesystem operations.

    This interface defines methods for common filesystem tasks such as listing files,
    deleting files, and opening files with given configurations.
    """
    
    @abstractmethod
    def get_files(self, prefix: str, additional_configs: dict = {}) -> List[str]:
        """
        Retrieve a list of file paths that match a given prefix.

        Parameters:
            prefix (str): The file path prefix or pattern to filter files.
            additional_configs (dict, optional): Additional configuration options that may 
                influence the file retrieval process (default is an empty dict).

        Returns:
            List[str]: A list of file paths that match the provided prefix.
        """
        pass

    @abstractmethod
    def delete_files(self, files: List[str], additional_configs: dict = {}) -> None:
        """
        Delete the specified files from the filesystem.

        Parameters:
            files (List[str]): A list of file paths to delete.
            additional_configs (dict, optional): Additional configuration options that may 
                influence the delete operation (default is an empty dict).

        Returns:
            None
        """
        pass
    
    @abstractmethod
    def open_file(self, path: str, mode: str, additional_configs: dict = {}) -> TextIOWrapper:
        """
        Open a file at the given path with the specified mode and return a file object.

        Parameters:
            path (str): The path to the file that should be opened.
            mode (str): The mode in which the file should be opened (e.g., 'r', 'w').
            additional_configs (dict, optional): Additional configuration options that may 
                affect the file opening process (default is an empty dict).

        Returns:
            TextIOWrapper: A file object corresponding to the opened file.
        """
        pass