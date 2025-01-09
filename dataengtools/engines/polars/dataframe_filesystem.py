import polars as pl
from typing import List
from dataengtools.core.interfaces.engine_layer.filesystem import Filesystem, FileMetadata
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler

class PolarsFilesystem(Filesystem[pl.DataFrame]):
    """Polars implementation of Filesystem interface."""
    
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
        
    def read_files(self, prefix: str, filetype: str, file_metadata: FileMetadata = {}) -> pl.DataFrame:
        """Read files into Polars DataFrame.
        
        Args:
            root: Base path containing the files
            prefix: File prefix/pattern to match
            filetype: Type of files ('parquet' or 'csv')
            
        Returns:
            Polars DataFrame containing file contents
            
        Raises:
            ValueError: If filetype is not supported
        """
        files = self.get_files(prefix)
        
        valid_filetypes = ['parquet', 'csv']
        if filetype.lower() not in valid_filetypes:
            raise ValueError(f"Unsupported file type: {filetype}")


        dfs = []
        for file in files:
            with self._handler.open_file(file, 'rb') as f:
                if filetype.lower() == 'parquet':
                    df = pl.read_parquet(f,
                                         columns=file_metadata.get('columns', None),
                                         hive_partitioning=file_metadata.get('hive_partitioning', False))
                elif filetype.lower() == 'csv':
                    df = pl.read_csv(f, 
                                     separator=file_metadata.get('separator', ','),
                                     has_header=file_metadata.get('header', True),
                                     columns=file_metadata.get('columns', None),
                                     encoding=file_metadata.get('encoding', 'utf-8'))
                else:
                    raise NotImplemented(f"The filetype {filetype} is not implemented.")
                dfs.append(df)
                
        if not dfs:
            return pl.DataFrame()
            
        return pl.concat(dfs)