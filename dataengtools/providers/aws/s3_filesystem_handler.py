from io import TextIOWrapper
from typing import List
from s3fs import S3FileSystem
import re
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler

class AWSS3FilesystemHandler(FilesystemHandler):
    """
    Implementation of FilesystemHandler for AWS S3 using S3FileSystem.
    """
    
    def __init__(self, fs: S3FileSystem):
        """
        Initialize the S3FilesystemHandler.
        
        Args:
            fs (S3FileSystem, optional): An instance of S3FileSystem. If None, creates a new instance.
        """
        self.fs = fs
        
    def _normalize_s3_path(self, path: str) -> str:
        """
        Normalize S3 path to ensure it starts with 's3://'
        
        Args:
            path (str): The S3 path to normalize
            
        Returns:
            str: Normalized S3 path
        """
        if not path.startswith('s3://'):
            return f's3://{path}'
        return path
    
    def get_files(self, prefix: str, additional_configs: dict = {}) -> List[str]:
        """
        List files in S3 with the given prefix.
        
        Args:
            prefix (str): The prefix to filter files
            additional_configs (dict): Additional configurations for S3 listing
                Supported configs:
                - pattern (str): Pattern to filter files
                
        Returns:
            List[str]: List of file paths
        """
        prefix = self._normalize_s3_path(prefix)
        
        # Extract configs with defaults
        pattern = additional_configs.get('pattern', None)
        
        try:
            # List files from S3
            files = self.fs.find(prefix, withdirs=False)
                        
            # Apply pattern filter if provided
            if pattern:
                pattern = re.compile(pattern)
                files = [f for f in files if pattern.search(f)]
                
            return [self._normalize_s3_path(f) for f in files]
            
        except Exception as e:
            raise Exception(f"Error listing files from S3 ") from e
    
    def delete_files(self, files: List[str], additional_configs: dict = {}) -> None:
        """
        Delete files from S3.
        
        Args:
            files (List[str]): List of file paths to delete
            additional_configs (dict): Additional configurations for deletion
                Supported configs:
                - recursive (bool): Whether to delete directories recursively
                - batch_size (int): Number of files to delete in each batch
        """
        if not files:
            return
            
        # Extract configs with defaults
        recursive = additional_configs.get('recursive', False)
        batch_size = additional_configs.get('batch_size', 1000)
        
        try:
            # Normalize paths
            normalized_files = [self._normalize_s3_path(f) for f in files]
            
            # Delete in batches to handle large numbers of files
            for i in range(0, len(normalized_files), batch_size):
                batch = normalized_files[i:i + batch_size]
                self.fs.rm(batch, recursive=recursive)
                
        except Exception as e:
            raise Exception(f"Error deleting files from S3") from e
    
    def open_file(self, path: str, mode: str, additional_configs: dict = {}) -> TextIOWrapper:
        """
        Open a file from S3.
        
        Args:
            path (str): Path to the file
            mode (str): File mode ('r', 'w', 'rb', 'wb', etc)
            additional_configs (dict): Additional configurations for file opening
                Supported configs:
                - encoding (str): File encoding
                - compression (str): Compression type
                - buffer_size (int): Buffer size for reading/writing
                
        Returns:
            TextIOWrapper: Opened file object
        """
        path = self._normalize_s3_path(path)
        
        # Extract configs with defaults
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
            ) # type: ignore
        except Exception as e:
            raise Exception(f"Error opening file from S3") from e