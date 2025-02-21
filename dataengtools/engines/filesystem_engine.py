from typing import List, Generator, overload, Any, Optional
from dataengtools.core.interfaces.engine_layer.filesystem import FilesystemEngine
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.io.reader import Reader, ReaderOptions
from dataengtools.core.interfaces.io.writer import Writer, WriterOptions

from duckdb import DuckDBPyRelation
import polars as pl

class DuckDBFilesystemEngine(FilesystemEngine[DuckDBPyRelation]):
    """
    Filesystem engine implementation using DuckDB for reading and writing files.

    This engine utilizes a FilesystemHandler for file operations, a Reader for reading files 
    into a DuckDBPyRelation, and a Writer for writing DuckDBPyRelation data to disk.
    """

    def __init__(self, handler: FilesystemHandler, reader: Reader[DuckDBPyRelation], writer: Writer[DuckDBPyRelation]):
        """
        Initialize the DuckDBFilesystemEngine with the required components.

        Parameters:
            handler (FilesystemHandler): The handler for performing file system operations.
            reader (Reader[DuckDBPyRelation]): The reader instance to load file contents into a DuckDBPyRelation.
            writer (Writer[DuckDBPyRelation]): The writer instance to persist DuckDBPyRelation data.
        """
        self._handler = handler
        self._reader = reader
        self._writer = writer
        
    def get_files(self, prefix: str) -> List[str]:
        """
        Retrieve a list of file paths from the filesystem matching the given prefix.

        Parameters:
            prefix (str): The file path prefix or pattern used to filter files.

        Returns:
            List[str]: A list of file paths.
        """
        return self._handler.get_files(prefix)
        
    def delete_files(self, files: List[str]) -> None:
        """
        Delete specified files from the filesystem.

        Parameters:
            files (List[str]): A list of file paths to be deleted.

        Returns:
            None
        """
        self._handler.delete_files(files)
        
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}) -> DuckDBPyRelation:
        """
        Read files from the filesystem and return the data as a DuckDBPyRelation.

        Parameters:
            prefix (str): The file path prefix or pattern to identify files.
            reader_options (ReaderOptions, optional): A dictionary of options that dictate how files are read.

        Returns:
            DuckDBPyRelation: The data read from the files.
        """
        data = self._reader.read(prefix, reader_options)
        return data
    
    def write_files(self, data: DuckDBPyRelation, path: str, writer_options: WriterOptions = {}) -> None:
        """
        Write the provided DuckDBPyRelation data to a specified path using given writer options.

        Parameters:
            data (DuckDBPyRelation): The data to be written.
            path (str): The destination file path.
            writer_options (WriterOptions, optional): A dictionary of options dictating the write operation.

        Returns:
            None
        """
        self._writer.write(data, path, writer_options)
    
class PolarsFilesystemEngine(DuckDBFilesystemEngine):
    """
    Filesystem engine implementation that returns data as Polars DataFrames.

    Extends DuckDBFilesystemEngine to enable file reading and writing where data is 
    represented by Polars DataFrames. Supports both full data loads and batched generators.
    """

    @overload
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}) -> pl.DataFrame:
        """
        Overload for reading files and returning a single Polars DataFrame.

        Parameters:
            prefix (str): The file path prefix or pattern to identify files.
            reader_options (ReaderOptions, optional): Options for reading the file(s).

        Returns:
            pl.DataFrame: The data read as a Polars DataFrame.
        """
        ...

    @overload
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}, *, batch_size: int) -> Generator[pl.DataFrame, None, None]:
        """
        Overload for reading files and returning a generator of Polars DataFrame batches.

        Parameters:
            prefix (str): The file path prefix or pattern to identify files.
            reader_options (ReaderOptions, optional): Options for reading the file(s).
            batch_size (int): The number of records per batch.

        Returns:
            Generator[pl.DataFrame, None, None]: A generator yielding batches as Polars DataFrames.
        """
        ...

    def read_files(
        self, 
        prefix: str, 
        reader_options: ReaderOptions = {}, 
        *, 
        batch_size: Optional[int] = None
    ) -> Any:
        """
        Read files from the filesystem and return the data as one or more Polars DataFrames.

        Parameters:
            prefix (str): The file path prefix or pattern to identify files.
            reader_options (ReaderOptions, optional): A dictionary of options for reading the file(s).
            batch_size (Optional[int]): If provided, returns a generator yielding batches of data;
                otherwise, returns a single Polars DataFrame.

        Returns:
            pl.DataFrame or Generator[pl.DataFrame, None, None]: The read data as a Polars DataFrame or as a generator of batches.
        """
        data = super().read_files(prefix, reader_options)
        if batch_size:
            return (pl.from_arrow(batch) for batch in data.fetch_arrow_reader(batch_size))
        else:
            return data.pl()
        
    def write_files(self, data: pl.DataFrame, path: str, writer_options: WriterOptions = {}) -> None:
        """
        Write a Polars DataFrame to a specified path using given writer options.

        Parameters:
            data (pl.DataFrame): The Polars DataFrame to be written.
            path (str): The destination file path.
            writer_options (WriterOptions, optional): Options for writing the file(s).

        Returns:
            None
        """
        return super().write_files(data, path, writer_options)  # type: ignore