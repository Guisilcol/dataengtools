from abc import ABC, abstractmethod
from typing import Optional, List, TypeVar, Generic, TypedDict

ResultSet = TypeVar('ResultSet')

class WriterOptions(TypedDict, total=False):
    """
    Dictionary type for specifying writer options.

    Attributes:
        separator (Optional[str]): The delimiter to use when writing files.
        file_type (Optional[str]): The type of file to be written (e.g., 'csv', 'parquet').
        columns (Optional[List[str]]): List of column names that will be written.
        order_by (Optional[List[str]]): List of columns defining the order of the output.
        has_header (Optional[bool]): Indicates whether the output file should include a header row.
        compression (Optional[str]): Compression format to apply (e.g., 'gzip').
        partition_by (Optional[List[str]]): List of columns to partition the output data.
        mode (Optional[str]): Write mode (e.g., 'overwrite', 'append').
    """
    separator: Optional[str]
    file_type: Optional[str]
    columns: Optional[List[str]]
    order_by: Optional[List[str]]
    has_header: Optional[bool]
    compression: Optional[str]
    partition_by: Optional[List[str]]
    file_type: Optional[str]
    mode: Optional[str]

class Writer(Generic[ResultSet], ABC):
    """
    Abstract base class for implementing file writers.

    Implementations of this class are responsible for writing data to a file located at a given path,
    using the options provided in WriterOptions.
    """

    @abstractmethod
    def write(self, data: ResultSet, path: str, writer_options: WriterOptions = {}) -> None:
        """
        Write data to a file at the specified path using the given writer options.

        Parameters:
            data (ResultSet): The data to be written to the file.
            path (str): The destination file path.
            writer_options (WriterOptions, optional): A dictionary of options that dictate the file format,
                ordering, compression, partitioning, and write mode. Defaults to an empty dictionary.

        Returns:
            None
        """
        pass