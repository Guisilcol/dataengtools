from typing import Literal, overload, Any
from polars import DataFrame
import boto3
from s3fs import S3FileSystem
import duckdb
from duckdb import DuckDBPyRelation

from dataengtools.core.interfaces.engine_layer.catalog import CatalogEngine
from dataengtools.core.interfaces.engine_layer.filesystem import FilesystemEngine
from dataengtools.core.interfaces.engine_layer.sql import SQLEngine
from dataengtools.providers.aws.glue_catalog_metadata_handler import AWSGlueTableMetadataRetriver
from dataengtools.providers.aws.glue_catalog_partitions_handler import AWSGluePartitionHandler
from dataengtools.providers.aws.s3_filesystem_handler import AWSS3FilesystemHandler
from dataengtools.providers.aws.glue_sql_provider_configurator import GlueSQLProviderConfigurator

from dataengtools.engines.catalog_engine import DuckDBCatalogEngine
from dataengtools.engines.filesystem_engine import DuckDBFilesystemEngine
from dataengtools.io.duckdb_io.reader import DuckDBReader


ProviderType = Literal['duckdb|aws', 'dataframe|aws']

class EngineFactory:
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
            sql_configurator = GlueSQLProviderConfigurator()

            return DuckDBCatalogEngine(
                partition_handler=AWSGluePartitionHandler(glue_cli, s3_cli),
                table_metadata_retriver=AWSGlueTableMetadataRetriver(glue_cli),
                filesystem=AWSS3FilesystemHandler(s3fs),
                reader=DuckDBReader(connection, sql_configurator)
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
    def get_filesystem_engine(provider: Literal['dataframe|aws'], configuration: dict = {}) -> Any:
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
            sql_configurator = GlueSQLProviderConfigurator()
            return DuckDBFilesystemEngine(AWSS3FilesystemHandler(s3fs), DuckDBReader(connection, sql_configurator))
        
        raise NotImplementedError(f'Filesystem engine for provider {provider} is not implemented')