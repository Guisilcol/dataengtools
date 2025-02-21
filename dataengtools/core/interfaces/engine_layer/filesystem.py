from abc import ABC, abstractmethod
from typing import TypeVar, List, Generic, TypedDict, Optional
from dataengtools.core.interfaces.io.reader import ReaderOptions

ResultSet = TypeVar('ResultSet')

class FileMetadata(TypedDict, total=False):
    """
    Dictionary type for file metadata.

    Attributes:
        separator (Optional[str]): The delimiter used in the file (e.g., comma, pipe).
        has_header (Optional[bool]): Indicates if the file contains a header row.
        skip_rows (Optional[int]): Number of rows to skip from the beginning of the file.
        n_rows (Optional[int]): Number of rows to read or expected number of rows.
        columns (Optional[List[str]]): List of column names.
        encoding (Optional[str]): File encoding (e.g., 'utf-8').
        compression (Optional[str]): Compression type, if any (e.g., 'gzip').
        hive_partitioning (Optional[bool]): Flag indicating if hive-style partitioning is used.
    """
    separator: Optional[str]
    has_header: Optional[bool]
    skip_rows: Optional[int]
    n_rows: Optional[int]
    columns: Optional[List[str]]
    encoding: Optional[str]
    compression: Optional[str]
    hive_partitioning: Optional[bool]

class FilesystemEngine(ABC, Generic[ResultSet]):
    """
    Abstract base class for filesystem engine implementations.

    FilesystemEngine defines an interface for operations on file systems,
    including listing files, deletion, and reading file contents.
    """

    @abstractmethod
    def get_files(self, prefix: str) -> List[str]:
        """
        List all file paths matching a given prefix in the filesystem.

        Parameters:
            prefix (str): The file prefix or pattern used to filter files.

        Returns:
            List[str]: A list of file paths that match the specified prefix.
        """
        pass

    @abstractmethod
    def delete_files(self, files: List[str]) -> None:
        """
        Delete specified files from the filesystem.

        Parameters:
            files (List[str]): A list of file paths to be deleted.

        Returns:
            None
        """
        pass
        
    @abstractmethod
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}) -> ResultSet:
        """
        Read files from the filesystem and return their contents in the 
        specified format.

        Parameters:
            prefix (str): The file prefix or pattern to search for files.
            reader_options (ReaderOptions, optional): Dictionary of options that 
                dictate how files should be read (e.g., file format, encoding). 
                Defaults to an empty dictionary.

        Returns:
            ResultSet: The combined result set containing the file contents
            as defined by the generic type.
        """
        pass