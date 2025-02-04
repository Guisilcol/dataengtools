from abc import ABC, abstractmethod
from typing import TypeVar, List, Generic, TypedDict, Optional

Frame = TypeVar('Frame')

class FileMetadata(TypedDict):
    """Dictionary type for file metadata."""
    separator: Optional[str]
    has_header: Optional[bool]
    skip_rows: Optional[int]
    n_rows: Optional[int]
    columns: Optional[List[str]]
    encoding: Optional[str]
    compression: Optional[str]
    hive_partitioning: Optional[bool]

class FilesystemEngine(ABC, Generic[Frame]):
    """Abstract interface for filesystem operations.
    
    Args:
        Frame: Generic type for the data structure returned by read_file
    """
    
    @abstractmethod
    def get_files(self, prefix: str) -> List[str]:
        """List all files under a path with given prefix.
        
        Args:
            root: Base path to search files
            prefix: File prefix/pattern to match
            
        Returns:
            List of file paths found
        """
        pass

    
    @abstractmethod
    def delete_files(self, files: List[str]) -> None:
        """Delete specified files from filesystem.
        
        Args:
            root: Base path containing the files
            files: List of file paths to delete
        """
        pass
        
    @abstractmethod
    def read_files(self, prefix: str, filetype: str, file_metadata: FileMetadata = {}) -> Frame:
        """Read a file and return its contents in specified format.
        
        Args:
            root: Base path containing the file
            prefix: File prefix/pattern to match
            filetype: Type/format of the file (e.g. 'parquet', 'csv')
            
        Returns:
            File contents in the specified generic type Frame
        """
        pass