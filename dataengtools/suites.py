
import boto3
from polars import DataFrame
from mypy_boto3_glue import GlueClient
from mypy_boto3_s3 import S3Client
from dataengtools.interfaces.catalog import Catalog
from dataengtools.aws.filesystem_handler import AWSS3FilesystemOperationsHandler
from dataengtools.aws.partition_handler import AWSGluePartitionHandler
from dataengtools.aws.table_metadata_retriver import AWSGlueTableMetadataRetriver
from dataengtools.polars_integration.catalog import DataFrameGlueCatalog

class PolarsSuite():
    def get_catalog(self,
                    glue_client: GlueClient = boto3.client('glue'), 
                    s3_client: S3Client = boto3.client('s3'),
    ) -> Catalog[DataFrame]:
        return DataFrameGlueCatalog(
            partition_handler=AWSGluePartitionHandler(glue_client, s3_client),
            file_handler=AWSS3FilesystemOperationsHandler(s3_client),
            table_metadata_retriver=AWSGlueTableMetadataRetriver(glue_client),            
        )
        