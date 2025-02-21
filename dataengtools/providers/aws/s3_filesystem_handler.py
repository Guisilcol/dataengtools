from io import TextIOWrapper
from typing import List
from s3fs import S3FileSystem
import re
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler

class AWSS3FilesystemHandler(FilesystemHandler):
    """
    AWS S3 implementation of FilesystemHandler using an S3FileSystem.

    This class provides methods to list, delete, and open files stored in AWS S3. 
    It ensures that S3 paths are normalized and supports additional configuration for filtering,
    recursion, and file opening options.
    """
    
    def __init__(self, fs: S3FileSystem):
        """
        Initialize the AWSS3FilesystemHandler with an S3FileSystem instance.

        Parameters:
            fs (S3FileSystem): An instance of S3FileSystem used for interacting with AWS S3.
        """
        self.fs = fs
        
    def _normalize_s3_path(self, path: str) -> str:
        """
        Normalize an S3 path to ensure it starts with 's3://'.

        Parameters:
            path (str): The S3 path to normalize.

        Returns:
            str: The normalized S3 path.
        """
        if not path.startswith('s3://'):
            return f's3://{path}'
        return path
    
    def get_files(self, prefix: str, additional_configs: dict = {}) -> List[str]:
        """
        List files in S3 matching a specified prefix.

        Parameters:
            prefix (str): The prefix used to filter files in S3.
            additional_configs (dict, optional): Additional configuration options for listing files.
                Supported options:
                    - pattern (str): A regex pattern to further filter file paths.

        Returns:
            List[str]: A list of normalized file paths that match the prefix and optional pattern.
        """
        prefix = self._normalize_s3_path(prefix)
        
        pattern = additional_configs.get('pattern', None)
        
        try:
            files = self.fs.find(prefix, withdirs=False)
                        
            if pattern:
                pattern = re.compile(pattern)
                files = [f for f in files if pattern.search(f)]
                
            return [self._normalize_s3_path(f) for f in files]
            
        except Exception as e:
            raise Exception("Error listing files from S3") from e
    
    def delete_files(self, files: List[str], additional_configs: dict = {}) -> None:
        """
        Delete a list of files from S3.

        Parameters:
            files (List[str]): A list of file paths to delete from S3.
            additional_configs (dict, optional): Additional options for deletion.
                Supported options:
                    - recursive (bool): If True, delete directories recursively.
                    - batch_size (int): The number of files to delete per batch (default is 1000).

        Returns:
            None
        """
        if not files:
            return
            
        recursive = additional_configs.get('recursive', False)
        batch_size = additional_configs.get('batch_size', 1000)
        
        try:
            normalized_files = [self._normalize_s3_path(f) for f in files]
            
            for i in range(0, len(normalized_files), batch_size):
                batch = normalized_files[i:i + batch_size]
                self.fs.rm(batch, recursive=recursive)
                
        except Exception as e:
            raise Exception("Error deleting files from S3") from e
    
    def open_file(self, path: str, mode: str, additional_configs: dict = {}) -> TextIOWrapper:
        """
        Open a file from S3 for reading or writing.

        Parameters:
            path (str): The S3 path to the file.
            mode (str): File mode (e.g., 'r', 'w', 'rb', 'wb').
            additional_configs (dict, optional): Additional options for opening the file.
                Supported options:
                    - encoding (str): The file encoding.
                    - compression (str): The compression type.
                    - buffer_size (int): Buffer size for reading or writing.

        Returns:
            TextIOWrapper: The file object for the opened file.
        """
        path = self._normalize_s3_path(path)
        
        encoding = additional_configs.get('encoding', None)
        compression = additional_configs.get('compression', None)
        buffer_size = additional_configs.get('buffer_size', None)
        
        try:
            return self.fs.open(
                path,
                mode=mode,
                encoding=encoding,
                compression=compression,
                buffer_size=buffer_size
            )  # type: ignore
        except Exception as e:
            raise Exception("Error opening file from S3") from e