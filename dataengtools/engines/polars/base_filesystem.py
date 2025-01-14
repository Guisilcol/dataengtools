from typing import List, TypeVar
from dataengtools.core.interfaces.engine_layer.filesystem import FilesystemEngine, FileMetadata
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler

Frame = TypeVar('Frame')

class FilesystemTemplate(FilesystemEngine[Frame]):
    """Polars implementation of FilesystemEngine interface."""
    
    def __init__(self, handler: FilesystemHandler):
        """Initialize with filesystem handler implementation.
        
        Args:
            handler: Implementation of FilesystemHandler interface
        """
        self._handler = handler
        
    def get_files(self, prefix: str) -> List[str]:
        """Get list of files using handler.
        
        Args:
            root: Base path to search files
            prefix: File prefix/pattern to match
            
        Returns:
            List of File objects
        """
        return self._handler.get_files(prefix)
        
    def delete_files(self, files: List[str]) -> None:
        """Delete files using handler.
        
        Args:
            root: Base path containing the files
            files: List of file paths to delete
        """
        self._handler.delete_files(files)
        
    def read_files(self, prefix: str, filetype: str, file_metadata: FileMetadata = {}) -> Frame:
        raise NotImplementedError("This class not have a concrete implementation of this method")