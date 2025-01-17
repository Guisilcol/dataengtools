from typing import TypeVar, Literal, overload
import polars as pl
import boto3
from s3fs import S3FileSystem
import duckdb
from duckdb import DuckDBPyConnection

from dataengtools.core.interfaces.engine_layer.catalog import CatalogEngine
from dataengtools.core.interfaces.engine_layer.filesystem import FilesystemEngine
from dataengtools.core.interfaces.engine_layer.sql import SQLEngine
from dataengtools.engines.polars.dataframe_catalog import PolarsDataFrameCatalog
from dataengtools.engines.polars.dataframe_filesystem import PolarsDataFrameFilesystem
from dataengtools.engines.polars.lazyframe_catalog import PolarsLazyFrameCatalog
from dataengtools.engines.polars.lazyframe_filesystem import PolarsLazyFrameFilesystem
from dataengtools.engines.duckdb.duckdb_sql import DuckDBEngine
from dataengtools.providers.aws.glue_catalog_metadata_handler import AWSGlueTableMetadataRetriver, AWSGlueDataTypeToPolars, AWSGlueDatabaseMetadataRetriever
from dataengtools.providers.aws.glue_catalog_partitions_handler import AWSGluePartitionHandler
from dataengtools.providers.aws.s3_filesystem_handler import AWSS3FilesystemHandler

ProviderType = Literal['dataframe|aws', 'lazyframe|aws']

class EngineFactory:
    @overload
    def get_catalog_engine(self, provider: Literal['dataframe|aws'], configuration: dict = {}) -> CatalogEngine[pl.DataFrame]:
        """
            Configuration is a dictionary that can contain the following keys:
                - glue_cli: boto3.client('glue') instance
                - s3_cli: boto3.client('s3') instance
                - s3fs: s3fs.S3FileSystem instance
        """
        pass


    @overload
    def get_catalog_engine(self, provider: Literal['lazyframe|aws'], configuration: dict = {}) -> CatalogEngine[pl.LazyFrame]:
        """
            Configuration is a dictionary that can contain the following keys:
                - glue_cli: boto3.client('glue') instance
                - s3_cli: boto3.client('s3') instance
                - s3fs: s3fs.S3FileSystem instance
        """

    def get_catalog_engine(self, provider: ProviderType, configuration: dict = {}) -> CatalogEngine:
        if provider == None:
            raise ValueError('Provider is required')

        if provider == 'dataframe|aws':
            glue_cli = configuration.get('glue_cli') or boto3.client('glue')
            s3_cli = configuration.get('s3_cli') or boto3.client('s3')
            s3fs = configuration.get('s3fs') or S3FileSystem()
            
            return PolarsDataFrameCatalog(
                datatype_mapping=AWSGlueDataTypeToPolars(),
                filesystem=AWSS3FilesystemHandler(s3fs),
                partition_handler=AWSGluePartitionHandler(glue_cli, s3_cli),
                table_metadata_retriver=AWSGlueTableMetadataRetriver(glue_cli)
            )
        
        if provider == 'lazyframe|aws':
            glue_cli = configuration.get('glue_cli') or boto3.client('glue')
            s3_cli = configuration.get('s3_cli') or boto3.client('s3')
            s3fs = configuration.get('s3fs') or S3FileSystem()
            
            return PolarsLazyFrameCatalog(
                datatype_mapping=AWSGlueDataTypeToPolars(),
                filesystem=AWSS3FilesystemHandler(s3fs),
                partition_handler=AWSGluePartitionHandler(glue_cli, s3_cli),
                table_metadata_retriver=AWSGlueTableMetadataRetriver(glue_cli)
            )
        
        raise NotImplementedError(f'CatalogEngine engine for provider {provider} is not implemented')

    @overload
    def get_filesystem_engine(self, provider: Literal['dataframe|aws'], configuration: dict = {}) -> FilesystemEngine[pl.DataFrame]: 
        """
        Configuration is a dictionary that can contain the following
        keys:
            - s3fs: s3fs.S3FileSystem instance
        """

    @overload
    def get_filesystem_engine(self, provider: Literal['lazyframe|aws'], configuration: dict = {}) -> FilesystemEngine[pl.LazyFrame]:
        """
        Configuration is a dictionary that can contain the following
        keys:
            - s3fs: s3fs.S3FileSystem instance
        """    
        pass
    

    def get_filesystem_engine(self, provider: ProviderType, configuration: dict = {}) -> FilesystemEngine:
        """
        Configuration is a dictionary that can contain the following
        keys:
            - s3fs: s3fs.S3FileSystem instance
        """
        if provider == None:
            raise ValueError('Provider is required')
        
        if provider == 'dataframe|aws':
            s3fs = configuration.get('s3fs') or S3FileSystem()
            return PolarsDataFrameFilesystem(handler=AWSS3FilesystemHandler(s3fs))
        
        if provider == 'lazyframe|aws':
            s3fs = configuration.get('s3fs') or S3FileSystem()
            return PolarsLazyFrameFilesystem(handler=AWSS3FilesystemHandler(s3fs))
        
        raise NotImplementedError(f'FilesystemEngine engine for provider {provider} is not implemented')

    def get_sql_engine(self, provider: Literal['duckdb|aws'], configuration: dict = {}) -> SQLEngine:
        """
        Configuration is a dictionary that can contain the following

        keys:
            - glue_cli: boto3.client('glue') instance
            - connection: duckdb.DuckDBPyConnection instance
            - database_metadata_retriever: DatabaseMetadataRetriever instance
            - table_metadata_retriever: TableMetadataRetriver instance
        """
        if provider == None:
            raise ValueError('Provider is required')
        
        if provider == 'duckdb|aws':
            glue_cli = configuration.get('glue_cli') or boto3.client('glue')
            connection = configuration.get('connection') or duckdb.connect(':memory:')
            database_metadata_retriever = configuration.get('database_metadata_retriever') or AWSGlueDatabaseMetadataRetriever(glue_cli)
            table_metadata_retriever = configuration.get('table_metadata_retriever') or AWSGlueTableMetadataRetriver(glue_cli)
            return DuckDBEngine(connection, database_metadata_retriever, table_metadata_retriever)
        
        raise NotImplementedError(f'SQLEngine engine for provider {provider} is not implemented')