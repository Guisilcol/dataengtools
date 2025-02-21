from duckdb import DuckDBPyConnection
from dataengtools.core.interfaces.integration_layer.sql_configurator import SQLProviderConfigurator

class GlueSQLProviderConfigurator(SQLProviderConfigurator[DuckDBPyConnection]):
    """
    SQL provider configurator for configuring a DuckDB connection to run in an AWS Glue environment.

    This configurator ensures that the DuckDB connection is set up with the proper directories 
    and secrets required to operate under AWS Glue, using the CREDENTIAL_CHAIN provider.
    """

    def configure_connection(self, connection: DuckDBPyConnection) -> DuckDBPyConnection:
        """
        Configure the given DuckDB connection for the AWS Glue environment.

        This method checks the current connection settings and, if the required AWS Glue settings 
        are not already set, it configures the 'home_directory', 'secret_directory', and 'extension_directory',
        and creates a secret using the S3 CREDENTIAL_CHAIN provider.

        Parameters:
            connection (DuckDBPyConnection): The DuckDB connection instance to configure.

        Returns:
            DuckDBPyConnection: The configured DuckDB connection.
        """
        data = connection.sql('SELECT name, value FROM duckdb_settings()').fetchall()
        settings = {key: value for key, value in data}

        if (settings.get('home_directory') == '/tmp' 
            and settings.get('secret_directory') == '/tmp/dataengtools_duckdb_secrets' 
            and settings.get('extension_directory') == '/tmp/dataengtools_duckdb_extensions'
        ):
            return connection

        connection.sql("SET home_directory='/tmp';")
        connection.sql("SET secret_directory='/tmp/dataengtools_duckdb_secrets';")
        connection.sql("SET extension_directory='/tmp/dataengtools_duckdb_extensions';")    
        connection.sql('CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);')
        return connection