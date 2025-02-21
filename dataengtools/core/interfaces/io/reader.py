from abc import ABC, abstractmethod
from typing import Optional, List, TypeVar, Generic, TypedDict

ResultSet = TypeVar('ResultSet')

class ReaderOptions(TypedDict, total=False):
    """
    Typed dictionary for specifying options for the file reader.

    Attributes:
        separator (Optional[str]): Delimiter used in files (e.g., ',' or '|').
        file_type (Optional[str]): File type (e.g., 'csv', 'txt', 'parquet').
        columns (Optional[List[str]]): List of column names to read from the file.
        condition (Optional[str]): Optional filter condition to apply when reading the data.
        order_by (Optional[List[str]]): List of columns to sort the data by.
        offset (Optional[int]): Number of records to skip at the beginning.
        limit (Optional[int]): Maximum number of records to read.
        has_header (Optional[bool]): Indicates whether the file includes a header row.
        skip_rows (Optional[int]): Number of rows to skip at the beginning of the file.
        n_rows (Optional[int]): Number of rows to read.
        encoding (Optional[str]): File encoding (e.g., 'utf-8').
        hive_partitioning (Optional[bool]): Specifies if hive-style partitioning is used.
    """
    separator: Optional[str]
    file_type: Optional[str]
    columns: Optional[List[str]]
    condition: Optional[str]
    order_by: Optional[List[str]]
    offset: Optional[int]
    limit: Optional[int]
    has_header: Optional[bool]
    skip_rows: Optional[int]
    n_rows: Optional[int]
    encoding: Optional[str]
    hive_partitioning: Optional[bool]


class Reader(Generic[ResultSet], ABC):
    """
    Abstract base class for implementing file readers.

    Readers are responsible for reading data from a file located at a given path,
    applying any specified reader options, and returning the data in a format defined
    by the generic type ResultSet.
    """

    @abstractmethod
    def read(self, path: str, writer_options: ReaderOptions = {}) -> ResultSet:
        """
        Read data from a file using the specified path and reader options.

        Parameters:
            path (str): The file path from which to read the data.
            writer_options (ReaderOptions, optional): A dictionary of options that
                dictate how the file should be read. Defaults to an empty dictionary.

        Returns:
            ResultSet: The data read from the file, structured as defined by the implementation.
        """
        pass