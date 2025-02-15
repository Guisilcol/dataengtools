from duckdb import DuckDBPyConnection, DuckDBPyRelation
from dataengtools.core.interfaces.engine_layer.sql import SQLEngine
from dataengtools.core.interfaces.integration_layer.sql_configurator import SQLProviderConfigurator
from dataengtools.utils.logger import Logger

LOGGER = Logger.get_instance()


class DuckDBSQLEngine(SQLEngine[DuckDBPyConnection, DuckDBPyRelation]):
    """DuckDB engine implementation for handling database operations"""

    def __init__(
        self,
        connection: DuckDBPyConnection,
        provider_configurator: SQLProviderConfigurator[DuckDBPyConnection]
    ):
        """Initialize DuckDB engine with necessary components"""
        self._connection = connection
        self._provider_configurator = provider_configurator

        self._configure_connection_to_run_in_aws()

    def _configure_connection_to_run_in_aws(self) -> None:        
        self._provider_configurator.configure_connection(self._connection)

    def get_connection(self) -> DuckDBPyConnection:
        """Get or create a DuckDB connection"""
        return self._connection

    def execute(self, query: str, params: dict = {}) -> None:
        """Execute a query with optional parameters"""
        params = params or {}
        self._connection.sql(query, params=params)

    def execute_and_fetch(self, query: str, params: dict = {}) -> DuckDBPyRelation:
        """Execute a query and return results as a DataFrame"""
        params = params or {}
        return self._connection.sql(query, params=params)