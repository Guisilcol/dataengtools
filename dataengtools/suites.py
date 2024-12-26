
import boto3
from polars import DataFrame
from s3fs import S3FileSystem
from typing import Literal
from dataengtools.interfaces.catalog import Catalog
from dataengtools.aws.filesystem_handler import AWSS3FilesystemHandler
from dataengtools.aws.partition_handler import AWSGluePartitionHandler
from dataengtools.aws.table_metadata_retriver import AWSGlueTableMetadataRetriver
from dataengtools.aws.datatype_mapping import AWSGlueDataTypeToPolars
from dataengtools.assets.polars_catalog import PolarsDataFrameCatalog


Types = Literal['polars_dataframe_aws']
    

class PolarsSuite():
    def get_catalog(self, type_: Types = 'polars_dataframe_aws', configuration: dict = {},) -> Catalog[DataFrame]:
        
        if type_ == 'polars_dataframe_aws':
            glue_client   = configuration.get('glue_client') or boto3.client('glue')
            s3_client     = configuration.get('s3_client') or boto3.client('s3')
            s3_filesystem = configuration.get('s3_filesystem') or S3FileSystem()
            
            return PolarsDataFrameCatalog(
                filesystem              = AWSS3FilesystemHandler(s3_client, s3_filesystem),
                partition_handler       = AWSGluePartitionHandler(glue_client, s3_client),
                table_metadata_retriver = AWSGlueTableMetadataRetriver(glue_client),
                datatype_mapping        = AWSGlueDataTypeToPolars()    
            )
            
        raise ValueError(f"Invalid type: {type_}'")
    
