import polars as pl
from dataengtools.core.interfaces.engine_layer.filesystem import FileMetadata
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.engines.polars.base_filesystem import FilesystemTemplate

class PolarsLazyFrameFilesystem(FilesystemTemplate[pl.LazyFrame]):
    """Polars implementation of Filesystem interface."""
    
    def __init__(self, handler: FilesystemHandler):
        """Initialize with filesystem handler implementation.
        
        Args:
            handler: Implementation of FilesystemHandler interface
        """
        super().__init__(handler)


    def read_files(self, prefix: str, filetype: str, file_metadata: FileMetadata = {}) -> pl.LazyFrame:
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
                    df = pl.scan_parquet(f)
                    if (cols := file_metadata.get('columns')):
                        df = df.select(*cols)

                elif filetype.lower() == 'csv':
                    df = pl.scan_csv(f,
                                     separator=file_metadata.get('separator', ','),
                                     has_header=file_metadata.get('has_header', True),
                                     encoding=file_metadata.get('encoding', 'utf8'),
                                     n_rows=file_metadata.get('n_rows', None),
                                     skip_rows=file_metadata.get('skip_rows', 0)
                    )

                    if (cols := file_metadata.get('columns')):
                        df = df.select(*cols)

                else:
                    raise NotImplemented(f"The filetype {filetype} is not implemented.")
                
                dfs.append(df)
                
        if not dfs:
            return pl.LazyFrame()
            
        return pl.concat(dfs)