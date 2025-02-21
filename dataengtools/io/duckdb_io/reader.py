from dataengtools.core.interfaces.io.reader import Reader, ReaderOptions
from dataengtools.io.duckdb_io.string_builder import StringBuilder
from dataengtools.core.interfaces.integration_layer.sql_configurator import SQLProviderConfigurator
from duckdb import DuckDBPyRelation, DuckDBPyConnection
from typing import Optional

class DuckDBReader(Reader[DuckDBPyRelation]):
    """
    DuckDBReader is responsible for reading files and transforming them into
    a DuckDBPyRelation using SQL queries.

    It leverages a DuckDB connection and a SQLProviderConfigurator to ensure
    the connection is configured for AWS or other environments as needed.
    """

    def __init__(self, connection: DuckDBPyConnection, sql_configurator: SQLProviderConfigurator[DuckDBPyConnection]):
        """
        Initialize the DuckDBReader with a DuckDB connection and a SQL provider configurator.

        Parameters:
            connection (DuckDBPyConnection): A DuckDB connection instance.
            sql_configurator (SQLProviderConfigurator[DuckDBPyConnection]): Configurator to adjust the connection settings.
        """
        self.connection = connection
        self.sql_configurator = sql_configurator
        self.sql_configurator.configure_connection(connection)

    def wrap_path_in_reader_function(self, 
                                     path: str, 
                                     file_type: Optional[str] = None, 
                                     have_header: Optional[bool] = None, 
                                     separator: Optional[str] = None
    ) -> str:
        """
        Wrap the given file path in a DuckDB reader function based on file type and options.

        This method constructs a SQL function call string to read the given file
        depending on the provided file type. For 'txt', a default delimiter is used;
        for 'csv' and 'parquet', appropriate read functions are used.

        Parameters:
            path (str): The file path to wrap.
            file_type (Optional[str]): The type of the file ('txt', 'csv', 'parquet').
            have_header (Optional[bool]): Flag indicating if the file has a header row.
            separator (Optional[str]): The column separator used in the file.

        Returns:
            str: A string representing the SQL reader function call.

        Raises:
            ValueError: If the provided file type is unsupported.
        """
        if file_type is None:
            return f"'{path}'"
        
        if file_type == 'txt':
            unique_separator = separator or chr(31)
            have_header = have_header or False
            return f"read_csv('{path}', delim = '{unique_separator}', header = {have_header}, columns = {{'value': 'VARCHAR'}})"

        if file_type == "csv":
            return f"read_csv('{path}', delim = '{separator}', header = {have_header})"
        
        if file_type == "parquet":
            return f"read_parquet('{path}')"
        
        raise ValueError(f"Unsupported file type: {file_type}")        

    def read(self, 
             path: str, 
             reader_options: ReaderOptions = {}
    ) -> DuckDBPyRelation:
        """
        Read data from a file and return it as a DuckDBPyRelation.

        This method uses the provided reader options to construct a SQL query that reads 
        the file. It supports various options like columns selection, filtering conditions,
        ordering, limiting, and offsetting the result set.

        Parameters:
            path (str): The file path to read data from.
            reader_options (ReaderOptions, optional): A dictionary of options that include:
                - file_type: The type of the file (e.g., 'csv', 'txt', 'parquet').
                - has_header: Boolean indicating if the file has a header.
                - separator: Delimiter used in the file.
                - columns: List of columns to be selected; defaults to ['*'] if not specified.
                - condition: SQL condition to filter rows.
                - order_by: List of columns to order by.
                - limit: Maximum number of rows to retrieve.
                - offset: Number of rows to skip.

        Returns:
            DuckDBPyRelation: The result of executing the constructed SQL query.
        """
        path = self.wrap_path_in_reader_function(
            path, 
            reader_options.get('file_type'),
            reader_options.get('has_header'),
            reader_options.get('separator')
        )

        sql = (
            StringBuilder()
            .append(f"SELECT {', '.join(reader_options.get('columns') or ['*'])}")
            .append(f"FROM {path}")
            .append(f"WHERE {reader_options.get('condition') or '1 = 1'}")
        )

        if (order_by := reader_options.get('order_by')):
            sql.append(f"ORDER BY {', '.join(order_by)}")

        if (limit := reader_options.get('limit')):
            sql.append(f"LIMIT {limit}")

        if (offset := reader_options.get('offset')):
            sql.append(f"OFFSET {offset}")

        return self.connection.sql(sql.build())