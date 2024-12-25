
import boto3
from polars import DataFrame
from mypy_boto3_glue import GlueClient
from mypy_boto3_s3 import S3Client
from s3fs import S3FileSystem
from dataengtools.interfaces.catalog import Catalog
from dataengtools.aws.filesystem_handler import AWSS3FilesystemHandler
from dataengtools.aws.partition_handler import AWSGluePartitionHandler
from dataengtools.aws.table_metadata_retriver import AWSGlueTableMetadataRetriver
from dataengtools.assets.polars_catalog import PolarsDataFrameCatalog


class PolarsSuite():
    def get_catalog(self,
                    glue_client: GlueClient = boto3.client('glue'), 
                    s3_client: S3Client = boto3.client('s3'),
                    s3_filesystem: S3FileSystem = S3FileSystem()
    ) -> Catalog[DataFrame]:
        return PolarsDataFrameCatalog(
            partition_handler=AWSGluePartitionHandler(glue_client, s3_client),
            file_handler=AWSS3FilesystemHandler(s3_client, s3_filesystem),
            table_metadata_retriver=AWSGlueTableMetadataRetriver(glue_client),            
        )
        