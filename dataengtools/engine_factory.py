from typing import Literal, overload, Any
import boto3
from s3fs import S3FileSystem
import duckdb

from dataengtools.core.interfaces.engine_layer.catalog import CatalogEngine
from dataengtools.core.interfaces.engine_layer.filesystem import FilesystemEngine
from dataengtools.providers.aws.glue_catalog_metadata_handler import AWSGlueTableMetadataRetriver
from dataengtools.providers.aws.glue_catalog_partitions_handler import AWSGluePartitionHandler
from dataengtools.providers.aws.s3_filesystem_handler import AWSS3FilesystemHandler
from dataengtools.providers.aws.glue_sql_provider_configurator import GlueSQLProviderConfigurator

from dataengtools.engines.catalog_engine import DuckDBCatalogEngine
from dataengtools.engines.filesystem_engine import DuckDBFilesystemEngine, PolarsFilesystemEngine
from dataengtools.engines.sql_engine import DuckDBSQLEngine
from dataengtools.io.duckdb_io.reader import DuckDBReader
from dataengtools.io.duckdb_io.writer import DuckDBWriter


ProviderType = Literal['duckdb|aws', 'dataframe|aws']

class EngineFactory:
    """
    EngineFactory is a factory class that provides methods to instantiate engine objects for SQL, catalog,
    and filesystem operations. Depending on the given provider and configuration, it returns the appropriate
    engine instance for interacting with DuckDB, AWS Glue, and S3.
    """

    @staticmethod
    def get_sql_engine(provider: str = 'duckdb|aws', configuration: dict = {}) -> DuckDBSQLEngine:
        """
        Creates and returns a SQL engine based on the provided provider and configuration.

        Parameters:
            provider (str): The provider type (default: 'duckdb|aws').
            configuration (dict): A dictionary that can include:
                - connection: a duckdb connection instance. If not provided, an in-memory DuckDB connection is used.

        Returns:
            DuckDBSQLEngine: An instance of DuckDBSQLEngine configured with the specified connection and
            a GlueSQLProviderConfigurator for AWS Glue integration.

        Raises:
            NotImplementedError: If the provider is not implemented.
        """
        if provider == 'duckdb|aws':
            connection = configuration.get('connection') or duckdb.connect(':memory:')
    
            return DuckDBSQLEngine(
                connection,
                GlueSQLProviderConfigurator()
            )
        
        raise NotImplementedError(f'SQL engine for provider {provider} is not implemented')


    @staticmethod
    @overload
    def get_catalog_engine(provider: Literal['duckdb|aws'], configuration: dict = {}) -> DuckDBCatalogEngine:
        """
        Creates and returns a catalog engine for the 'duckdb|aws' provider.

        Configuration dictionary may contain the following keys:
            - glue_cli: boto3.client('glue') instance.
            - s3_cli: boto3.client('s3') instance.
            - s3fs: s3fs.S3FileSystem instance.

        Returns:
            DuckDBCatalogEngine: An engine for handling catalog operations in AWS Glue using DuckDB.
        """
        pass

    @staticmethod
    @overload
    def get_catalog_engine(provider: Literal['dataframe|aws'], configuration: dict = {}) -> Any:
        """
        Creates and returns a catalog engine for the 'dataframe|aws' provider.

        Configuration dictionary may contain the following keys:
            - glue_cli: boto3.client('glue') instance.
            - s3_cli: boto3.client('s3') instance.
            - s3fs: s3fs.S3FileSystem instance.

        Returns:
            Any: An engine instance for handling catalog operations for the dataframe AWS provider.
        """
        pass

    @staticmethod
    def get_catalog_engine(provider: ProviderType, configuration: dict = {}) -> CatalogEngine:
        """
        Creates and returns a catalog engine based on the provided provider and configuration.

        Parameters:
            provider (ProviderType): Either 'duckdb|aws' or 'dataframe|aws'.
            configuration (dict): A dictionary that can include:
                - glue_cli: boto3.client('glue') instance.
                - s3_cli: boto3.client('s3') instance.
                - s3fs: s3fs.S3FileSystem instance.
                - connection: duckdb connection instance (for DuckDBCatalogEngine).

        Returns:
            CatalogEngine: An appropriate catalog engine instance.

        Raises:
            NotImplementedError: If the provider engine is not implemented.
        """
        if provider == 'duckdb|aws':
            glue_cli = configuration.get('glue_cli') or boto3.client('glue')  # type: ignore
            s3_cli = configuration.get('s3_cli') or boto3.client('s3')  # type: ignore
            s3fs = configuration.get('s3fs') or S3FileSystem()
            connection = configuration.get('connection') or duckdb.connect(':memory:')

            return DuckDBCatalogEngine(
                partition_handler=AWSGluePartitionHandler(glue_cli, s3_cli),
                table_metadata_retriver=AWSGlueTableMetadataRetriver(glue_cli),
                filesystem=AWSS3FilesystemHandler(s3fs),
                reader=DuckDBReader(connection, GlueSQLProviderConfigurator())
            )
        
        raise NotImplementedError(f'CatalogEngine engine for provider {provider} is not implemented')
    

    @staticmethod
    @overload
    def get_filesystem_engine(provider: Literal['duckdb|aws'], configuration: dict = {}) -> DuckDBFilesystemEngine:
        """
        Creates and returns a filesystem engine for the 'duckdb|aws' provider.

        Configuration dictionary may contain the following keys:
            - s3fs: s3fs.S3FileSystem instance.
            - connection: duckdb.DuckDBPyConnection instance.

        Returns:
            DuckDBFilesystemEngine: An engine for handling filesystem operations using DuckDB and AWS S3.
        """
        pass

    @staticmethod
    @overload
    def get_filesystem_engine(provider: Literal['dataframe|aws'], configuration: dict = {}) -> PolarsFilesystemEngine:
        """
        Creates and returns a filesystem engine for the 'dataframe|aws' provider.

        Configuration dictionary may contain the following keys:
            - s3fs: s3fs.S3FileSystem instance.
            - reader_connection: duckdb.DuckDBPyConnection instance for reading.
            - writer_connection: duckdb.DuckDBPyConnection instance for writing.

        Returns:
            PolarsFilesystemEngine: An engine for handling filesystem operations using Polars,
            DuckDB for I/O and AWS S3.
        """
        pass

    @staticmethod
    def get_filesystem_engine(provider: str, configuration: dict = {}) -> FilesystemEngine:
        """
        Creates and returns a filesystem engine based on the provided provider and configuration.

        Parameters:
            provider (str): The provider type (e.g., 'duckdb|aws' or 'dataframe|aws').
            configuration (dict): A dictionary that can include:
                For 'duckdb|aws':
                    - s3fs: s3fs.S3FileSystem instance.
                    - connection: duckdb.DuckDBPyConnection instance.
                For 'dataframe|aws':
                    - s3fs: s3fs.S3FileSystem instance.
                    - reader_connection: duckdb.DuckDBPyConnection instance for reading.
                    - writer_connection: duckdb.DuckDBPyConnection instance for writing.

        Returns:
            FilesystemEngine: An appropriate filesystem engine instance configured for AWS S3 operations.
        
        Raises:
            NotImplementedError: If the provider filesystem engine is not implemented.
        """
        if provider == 'duckdb|aws':
            s3fs = configuration.get('s3fs') or S3FileSystem()
            connection = configuration.get('connection') or duckdb.connect(':memory:')
            # It is important that DuckDBReader and DuckDBWriter receive the same connection,
            # because attempting to register a relation from one connection to another can raise an error.
            return DuckDBFilesystemEngine(
                AWSS3FilesystemHandler(s3fs), 
                DuckDBReader(connection, GlueSQLProviderConfigurator()),
                DuckDBWriter(connection, GlueSQLProviderConfigurator())
            )
        
        if provider == 'dataframe|aws':
            s3fs = configuration.get('s3fs') or S3FileSystem()
            reader_connection = configuration.get('reader_connection') or duckdb.connect(':memory:')
            writer_connection = configuration.get('writer_connection') or duckdb.connect(':memory:')
            # It is important that DuckDBReader and DuckDBWriter receive separate connections,
            # because if you try to write a dataframe read in batches using the same connection,
            # all subsequent batches after the write operation will be lost. Reason for this is unknown.
            return PolarsFilesystemEngine(
                AWSS3FilesystemHandler(s3fs), 
                DuckDBReader(reader_connection, GlueSQLProviderConfigurator()),
                DuckDBWriter(writer_connection, GlueSQLProviderConfigurator())
            )

        raise NotImplementedError(f'Filesystem engine for provider {provider} is not implemented')