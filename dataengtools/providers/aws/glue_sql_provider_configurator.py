from duckdb import DuckDBPyConnection
from dataengtools.core.interfaces.integration_layer.sql_configurator import SQLProviderConfigurator

class GlueSQLProviderConfigurator(SQLProviderConfigurator[DuckDBPyConnection]):
    """Glue SQL provider configurator for configuring DuckDB connection to run in AWS Glue environment"""

    def configure_connection(self, connection: DuckDBPyConnection) -> DuckDBPyConnection:
        """Configure Glue SQL connection"""
        connection.sql("SET home_directory='/tmp';")
        connection.sql("SET secret_directory='/tmp/dataengtools_duckdb_secrets';")
        connection.sql("SET extension_directory='/tmp/dataengtools_duckdb_extensions';")
        connection.sql('CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);')
        return connection