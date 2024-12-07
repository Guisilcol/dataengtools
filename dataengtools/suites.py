
import boto3
from mypy_boto3_glue import GlueClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_athena import AthenaClient
from dataengtools.pandas_integration.catalog import GlueCatalogWithPandas
from dataengtools.interfaces.catalog import Catalog
from pandas import DataFrame

class PandasSuite():
    def get_catalog(self, 
                    athena_s3_output_path: str, 
                    glue_client: GlueClient = boto3.client('glue'), 
                    s3_client: S3Client = boto3.client('s3'), 
                    athena_client: AthenaClient = boto3.client('athena')
    ) -> Catalog[DataFrame]:
        return GlueCatalogWithPandas(glue_client, s3_client, athena_client, athena_s3_output_path)