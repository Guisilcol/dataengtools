from duckdb import DuckDBPyConnection, DuckDBPyRelation
from dataengtools.core.interfaces.engine_layer.sql import SQLEngine
from dataengtools.core.interfaces.integration_layer.sql_configurator import SQLProviderConfigurator
from dataengtools.utils.logger import Logger


class DuckDBSQLEngine(SQLEngine[DuckDBPyConnection, DuckDBPyRelation]):
    """
    DuckDB SQLEngine implementation for handling database operations in an AWS environment.

    This engine leverages a DuckDB connection and a provider configurator to execute queries and manage
    SQL operations.
    """

    def __init__(
        self,
        connection: DuckDBPyConnection,
        provider_configurator: SQLProviderConfigurator[DuckDBPyConnection]
    ):
        """
        Initialize the DuckDBSQLEngine with a DuckDB connection and a SQL provider configurator.

        Parameters:
            connection (DuckDBPyConnection): A DuckDB connection instance.
            provider_configurator (SQLProviderConfigurator[DuckDBPyConnection]): An instance 
                for configuring the DuckDB connection for AWS integration.
        """
        self._connection = connection
        self._provider_configurator = provider_configurator

        self._configure_connection_to_run_in_aws()

    def _configure_connection_to_run_in_aws(self) -> None:
        """
        Configure the DuckDB connection to operate in an AWS environment.

        This method uses the provided SQLProviderConfigurator to set up the connection with
        AWS-specific settings.
        """
        self._provider_configurator.configure_connection(self._connection)

    def get_connection(self) -> DuckDBPyConnection:
        """
        Retrieve the current DuckDB connection instance.

        Returns:
            DuckDBPyConnection: The active DuckDB connection.
        """
        return self._connection

    def execute(self, query: str, params: dict = {}) -> None:
        """
        Execute a SQL query without returning any results.

        Parameters:
            query (str): The SQL statement to execute.
            params (dict, optional): A dictionary of parameters to bind to the query. Defaults to an empty dict.

        Returns:
            None
        """
        params = params or {}
        self._connection.sql(query, params=params)

    def execute_and_fetch(self, query: str, params: dict = {}) -> DuckDBPyRelation:
        """
        Execute a SQL query and return the result set.

        Parameters:
            query (str): The SQL statement to execute.
            params (dict, optional): A dictionary of parameters to bind to the query. Defaults to an empty dict.

        Returns:
            DuckDBPyRelation: The result of the executed query as a DuckDBPyRelation.
        """
        params = params or {}
        return self._connection.sql(query, params=params)