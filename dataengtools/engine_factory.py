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

    @staticmethod
    def get_sql_engine(provider: str = 'duckdb|aws', configuration: dict = {}) -> DuckDBSQLEngine:
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
            Configuration is a dictionary that can contain the following keys:
                - glue_cli: boto3.client('glue') instance
                - s3_cli: boto3.client('s3') instance
                - s3fs: s3fs.S3FileSystem instance
        """
        pass

    @staticmethod
    @overload
    def get_catalog_engine(provider: Literal['dataframe|aws'], configuration: dict = {}) -> Any:
        """
            Configuration is a dictionary that can contain the following keys:
                - glue_cli: boto3.client('glue') instance
                - s3_cli: boto3.client('s3') instance
                - s3fs: s3fs.S3FileSystem instance
        """

    @staticmethod
    def get_catalog_engine(provider: ProviderType, configuration: dict = {}) -> CatalogEngine:
        if provider == 'duckdb|aws':
            glue_cli = configuration.get('glue_cli') or boto3.client('glue') # type: ignore
            s3_cli = configuration.get('s3_cli') or boto3.client('s3') # type: ignore
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
            Configuration is a dictionary that can contain the following keys:
                - s3fs: s3fs.S3FileSystem instance
                - connection: duckdb.DuckDBPyConnection instance
        """
        pass

    @staticmethod
    @overload
    def get_filesystem_engine(provider: Literal['dataframe|aws'], configuration: dict = {}) -> PolarsFilesystemEngine:
        """
            Configuration is a dictionary that can contain the following keys:
                - s3fs: s3fs.S3FileSystem instance
                - connection: duckdb.DuckDBPyConnection instance
        """

    @staticmethod
    def get_filesystem_engine(provider: str, configuration: dict = {}) -> FilesystemEngine:
        if provider == 'duckdb|aws':
            s3fs = configuration.get('s3fs') or S3FileSystem()
            connection = configuration.get('connection') or duckdb.connect(':memory:')
            # It is important that DuckDBReader and DuckDBWriter receive a same connection,
            # because if you try to register a relation from a connection to another, it will raise an error.
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