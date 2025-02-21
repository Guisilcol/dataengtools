from dataengtools.core.interfaces.io.writer import Writer, WriterOptions
from dataengtools.io.duckdb_io.string_builder import StringBuilder
from dataengtools.core.interfaces.integration_layer.sql_configurator import SQLProviderConfigurator
from duckdb import DuckDBPyRelation, DuckDBPyConnection
import uuid


class DuckDBWriter(Writer[DuckDBPyRelation]):
    """
    DuckDBWriter is responsible for writing a DuckDBPyRelation to disk.

    This implementation leverages a DuckDB connection along with a SQLProviderConfigurator
    to configure the connection for the target environment. Data is written by constructing
    a SQL COPY command using the provided writer options.
    """

    def __init__(self, connection: DuckDBPyConnection, sql_configurator: SQLProviderConfigurator[DuckDBPyConnection]):
        """
        Initialize the DuckDBWriter with a DuckDB connection and a SQL provider configurator.

        Parameters:
            connection (DuckDBPyConnection): The DuckDB connection instance.
            sql_configurator (SQLProviderConfigurator[DuckDBPyConnection]): Configurator to adjust the connection's settings.
        """
        self.connection = connection
        self.sql_configurator = sql_configurator
        self.sql_configurator.configure_connection(connection)

    def write(self, data: DuckDBPyRelation, path: str, writer_options: WriterOptions = {}) -> None:
        """
        Write the given DuckDBPyRelation data to a file using a SQL COPY command.

        The method constructs a SQL statement based on the provided writer options which include:
            - columns: List of columns to be written, defaulting to '*' if not specified.
            - file_type: The output file type (e.g., 'parquet'); defaults to 'parquet'.
            - partition_by: Optional list of columns to partition the output data.
            - mode: Write mode (e.g., 'OVERWRITE' or 'APPEND'); defaults to 'OVERWRITE'.

        If no partitioning is specified, a unique filename is generated.

        Parameters:
            data (DuckDBPyRelation): The data to be written.
            path (str): The destination directory or file path prefix.
            writer_options (WriterOptions, optional): Options controlling the write operation.

        Returns:
            None
        """
        columns = ", ".join(writer_options.get('columns') or ['*'])
        filetype = writer_options.get('file_type') or 'parquet'
        partition_by = writer_options.get('partition_by')
        mode = writer_options.get('mode') or 'OVERWRITE'

        if not partition_by:
            name = str(uuid.uuid4().hex)
            path = path + f'/{name}.' + filetype.lower()

        sql = (
            StringBuilder()
            .append('COPY')
            .append(f'(SELECT {columns} FROM data)')
            .append(f"TO '{path}'")
            .append(f'(FORMAT {filetype}')
            .append(f', PARTITION_BY ({", ".join(partition_by)})' if partition_by else '')
            .append(f', {mode});')
            .build()
        )

        self.connection.register('data', data)
        self.connection.sql(sql)
        self.connection.unregister('data')