import boto3
from s3fs import S3FileSystem

from dataengtools.core.interfaces.engine_layer.catalog import Catalog
from dataengtools.core.interfaces.engine_layer.filesystem import Filesystem
from dataengtools.engines.polars.dataframe_catalog import PolarsDataFrameCatalog
from dataengtools.engines.polars.dataframe_filesystem import PolarsFilesystem
from dataengtools.providers.aws.glue_catalog_metadata_handler import AWSGlueTableMetadataRetriver, AWSGlueDataTypeToPolars
from dataengtools.providers.aws.glue_catalog_partitions_handler import AWSGluePartitionHandler
from dataengtools.providers.aws.s3_filesystem_handler import AWSS3FilesystemHandler


class EngineFactory:

    def get_catalog_engine(self, provider: str = 'dataframe|aws', configuration: dict = {}) -> Catalog:
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
        
        raise NotImplementedError(f'Catalog engine for provider {provider} is not implemented')


    def get_filesystem_engine(self, provider: str = 'dataframe|aws', configuration: dict = {}) -> Filesystem:
        if provider == 'dataframe|aws':
            s3fs = configuration.get('s3fs') or S3FileSystem()

            return PolarsFilesystem(
                handler=AWSS3FilesystemHandler(s3fs)
            )
        
        raise NotImplementedError(f'Filesystem engine for provider {provider} is not implemented')