from mypy_boto3_glue import GlueClient
from botocore.exceptions import ClientError
from dataengtools.interfaces.metadata import TableMetadataRetriver, TableMetadata, Column

class AWSGlueTableMetadataRetriver(TableMetadataRetriver):
    """
    Implementation of TableMetadataRetriver for AWS Glue.
    """
    
    INPUT_FORMAT_TO_FILE_TYPE = {
        'org.apache.hadoop.mapred.TextInputFormat': 'csv',
        'org.apache.hadoop.mapred.SequenceFileInputFormat': 'sequence',
        'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat': 'orc',
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat': 'parquet',
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat': 'avro',
        'org.apache.hadoop.hive.ql.io.avro.AvroKeyInputFormat': 'avro',
    }
    
    def __init__(self, glue: GlueClient) -> None:
        self.glue = glue
    
    def get_table_metadata(self, database: str, table: str) -> TableMetadata:
        """
        Retrieve metadata for a specific table in a database.

        :param database: The name of the database.
        :param table: The name of the table.
        :return: TableMetadata object containing metadata of the table.
        """
        
        try:
            response = self.glue.get_table(DatabaseName=database, Name=table)
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                raise ValueError(f"Table {table} not found in database {database}") from e
            else:
                raise e
                
        columns = [Column(name = col['Name'], data_type=col['Type']) for col in response['Table']['StorageDescriptor']['Columns']]
        partition_columns = [Column(name = col['Name'], data_type=col['Type']) for col in response['Table'].get('PartitionKeys', [])]
        all_columns = columns + partition_columns
        location = response['Table']['StorageDescriptor']['Location']
        
        serde_params = response['Table']['StorageDescriptor'].get('SerdeInfo', {}).get('Parameters', {})
        
        columns_separator = serde_params.get('field.delim') or serde_params.get('separatorChar')
        files_have_header = serde_params.get('skip.header.line.count', '0') != '0'
        files_extension = self.INPUT_FORMAT_TO_FILE_TYPE.get(response['Table']['StorageDescriptor']['InputFormat'], 'unknown')
        
        raw_metadata = response['Table']
        source = 'AWS Glue'
        
        
        return TableMetadata(
            database=database,
            table=table,
            columns=columns,
            partition_columns=partition_columns,
            all_columns=all_columns,
            location=location,
            files_have_header=files_have_header,
            files_extension=files_extension,
            columns_separator=columns_separator,
            raw_metadata=raw_metadata,
            source=source
        )
        
        