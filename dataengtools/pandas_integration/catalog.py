from abc import ABC, abstractmethod
import time
from typing import Tuple, List, Optional
import pandas as pd
from mypy_boto3_glue import GlueClient
from mypy_boto3_glue.type_defs import PartitionTypeDef, TableTypeDef, StorageDescriptorTypeDef
from mypy_boto3_s3 import S3Client
from mypy_boto3_athena import AthenaClient
import dataengtools.interfaces as interfaces
from dataengtools.aws_utils.s3 import S3Utils

class _Reader(ABC):
    @abstractmethod
    def read(self, 
             s3_path: str, 
             storage_descriptor: StorageDescriptorTypeDef, 
             columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        pass


class _ParquetReader(_Reader):
    def read(self, 
             s3_path: str, 
             storage_descriptor: StorageDescriptorTypeDef, 
             columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        return pd.read_parquet(s3_path, columns=columns)

    
class _CSVReader(_Reader):
    def read(self, 
             s3_path: str, 
             storage_descriptor: StorageDescriptorTypeDef, 
             columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        sep = storage_descriptor.get('SerdeInfo', {}).get('Parameters', {}).get('field.delim', ',')
        header = storage_descriptor.get('SerdeInfo', {}).get('Parameters', {}).get('skip.header.line.count', 0)
        return pd.read_csv(s3_path, sep=sep, header=header)


class _ReaderFactory:
    _readers = {
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat': _ParquetReader,
        'org.apache.hadoop.mapred.TextInputFormat': _CSVReader,
    }

    @staticmethod
    def get_reader(input_format: str) -> _Reader:
        reader_class = _ReaderFactory._readers.get(input_format)
        if not reader_class:
            raise ValueError(f"Unsupported input format: {input_format}")
        return reader_class()


class _Writer(ABC):
    @abstractmethod
    def write(self, df: pd.DataFrame, s3_path: str, storage_descriptor: StorageDescriptorTypeDef) -> None:
        pass

    
class _ParquetWriter(_Writer):
    def write(self, df: pd.DataFrame, s3_path: str, storage_descriptor: StorageDescriptorTypeDef) -> None:
        compression = storage_descriptor.get('SerdeInfo', {}).get('Parameters', {}).get('compressionType', 'snappy')
        df.to_parquet(s3_path, compression=compression)

        
class _CSVWriter(_Writer):
    def write(self, df: pd.DataFrame, s3_path: str, storage_descriptor: StorageDescriptorTypeDef) -> None:
        sep = storage_descriptor.get('SerdeInfo', {}).get('Parameters', {}).get('separatorChar', ',')
        header = storage_descriptor.get('SerdeInfo', {}).get('Parameters', {}).get('skip.header.line.count', 0)
        quoting = storage_descriptor.get('SerdeInfo', {}).get('Parameters', {}).get('quoteChar', '"')
        df.to_csv(s3_path, sep=sep, header=header, index=False, quoting=quoting)


class GlueCatalogWithPandas(interfaces.Catalog[pd.DataFrame]):
    OUTPUT_FORMAT_TO_WRITER = {
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat': _ParquetWriter().write,
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat': _CSVWriter().write,
    }
    
    CATALOG_DATATYPE_TO_PANDAS = {
        'string': 'object',
        'int': 'int64',
        'bigint': 'int64',
        'float': 'float64',
        'double': 'float64',
        'boolean': 'bool',
        'timestamp': 'datetime',
        'date': 'datetime',
    }

    def __init__(self, glue_client: GlueClient, s3_client: S3Client, athena_client: AthenaClient, athena_output_s3_path: str) -> None:
        self.glue_client = glue_client
        self.s3_client = s3_client
        self.athena_client = athena_client
        self.athena_output_s3_path = athena_output_s3_path
        
    def _create_s3_path(self, bucket: str, prefix: str) -> str:
        return f's3://{bucket}/{prefix}'
        
    def _get_partitions(self, db: str, table: str, conditions: str) -> List[PartitionTypeDef]:
        paginator = self.glue_client.get_paginator('get_partitions')
        response_iterator = paginator.paginate(DatabaseName=db, TableName=table, Expression=conditions)
        
        partitions = []
        for response in response_iterator:
            for partition in response['Partitions']:
                partitions.append(partition)
                
        return partitions
    
    def _get_table(self, db: str, table: str) -> TableTypeDef:
        return self.glue_client.get_table(DatabaseName=db, Name=table)['Table']
    
    def _repair_table(self, db: str, table: str) -> None:
        query = f'MSCK REPAIR TABLE {db}.{table}'
        
        running_query = self.athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': self.athena_output_s3_path,
            }
        )
        
        execution_id = running_query['QueryExecutionId']
        
        while True:
            stats = self.athena.get_query_execution(execution_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED']:
                break
            
            if status in ['FAILED', 'CANCELLED']:
                raise ValueError(f'The query {query} execution failed with status {status}')
            
            time.sleep(0.2)  # 200ms
    
    def _read_data(self, location: str, input_format: str, storage_descriptor: StorageDescriptorTypeDef, columns: Optional[List[str]]) -> pd.DataFrame:
        reader = _ReaderFactory.get_reader(input_format)
        if reader is None:
            raise ValueError(f"Unsupported input format: {input_format}")
        
        bucket, prefix = S3Utils.get_bucket_and_prefix(location)
        files = S3Utils.get_keys_from_prefix(self.s3_client, bucket, prefix)
        
        dfs = [reader.read(self._create_s3_path(bucket, file), storage_descriptor, columns) for file in files]
        return pd.concat(dfs)
        
    def get_location(self, db: str, table: str) -> str:
        return self._get_table(db, table)['StorageDescriptor']['Location']
    
    def read_table(self, db: str, table: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        metadata = self._get_table(db, table)
        location = metadata['StorageDescriptor']['Location']
        input_format = metadata['StorageDescriptor']['InputFormat']
        return self._read_data(location, input_format, metadata['StorageDescriptor'], columns)

    
    def read_partitioned_table(self, 
                               db: str, 
                               table: str, 
                               conditions: str, 
                               columns: Optional[List[str]] = None
    ) -> pd.DataFrame: 
        # Get all partitions that match the conditions via Glue client, then get the location of each partition and read the data
        
        partitions = self._get_partitions(db, table, conditions)
        
        dfs = []
        for partition in partitions:
            location = partition['StorageDescriptor']['Location']
            input_format = partition['StorageDescriptor']['InputFormat']
            
            reader = _ReaderFactory.get_reader(input_format)
            
            if reader is None:
                raise ValueError(f'The table have a unsupported input format: {input_format}')
            
            bucket, prefix = S3Utils.get_bucket_and_prefix(location)            
            partition_columns = self.get_partition_columns(db, table)            
            partition_path = '/'.join(f'{column}={value}' for column, value in zip(partition_columns, partition['Values']))
            full_prefix = f'{prefix}/{partition_path}'
            s3_path = self._create_s3_path(bucket, full_prefix)
            dfs.append(
                self._read_data(s3_path, input_format, partition['StorageDescriptor'], columns)
            )
            
                
        return pd.concat(dfs)
        
    def adapt_frame_to_table_schema(self, df: pd.DataFrame, db: str, table: str) -> pd.DataFrame:
        metadata = self._get_table(db, table)
        schema = metadata['StorageDescriptor']['Columns']
        adapted_df = pd.DataFrame()

        for column in schema:
            column_name = column['Name']
            pandas_dtype = self.CATALOG_DATATYPE_TO_PANDAS.get(column['Type'], 'object')
            
            if column_name not in df.columns:
                adapted_df[column_name] = pd.Series(dtype=pandas_dtype)
            else:
                adapted_df[column_name] = df[column_name].astype(pandas_dtype)

        return adapted_df
    
    def get_partition_columns(self, db: str, table: str) -> List[str]:
        partitions =  self._get_table(db, table)['PartitionKeys']
        return [partition['Name'] for partition in partitions]
    
    def write_table(self, df: pd.DataFrame, db: str, table: str, overwrite: bool = True) -> None:
        table = self._get_table(db, table)   
        location = table['StorageDescriptor']['Location']
        bucket, prefix = S3Utils.get_bucket_and_prefix(location)
        
        partitions_columns = self.get_partition_columns(db, table)
                
        # Is table partitioned?
        if partitions_columns:
            # Write the DataFrame to the table location, creating a partition for each unique combination of partition columns
            for grouped_df, values in df.groupby(partitions_columns):
                
                if overwrite:
                    # Delete the partition if it already exists
                    partitions = self._get_partitions(db, table, ' AND '.join(f'{column}={value}' for column, value in zip(partitions_columns, values)))
                    for partition in partitions:
                        self.glue_client.delete_partition(DatabaseName=db, TableName=table, PartitionValues=partition['Values'])
                        
                    # Delete the data in the S3 location
                    
                    s3_path = self._create_s3_path(bucket, f'{prefix}/{partition}')
                    files = S3Utils.get_keys_from_prefix(self.s3_client, bucket, s3_path)
                    for file in files:
                        self.s3_client.delete_object(Bucket=bucket, Key=file)                
                
                partition = '/'.join(f'{column}={value}' for column, value in zip(partitions_columns, values))
                s3_path = self._create_s3_path(bucket, f'{prefix}/{partition}')
                writer = self.OUTPUT_FORMAT_TO_WRITER.get(table['StorageDescriptor']['OutputFormat'])
                if writer is None:
                    raise ValueError(f'The table have a unsupported output format: {table["StorageDescriptor"]["OutputFormat"]}')
                writer(grouped_df, s3_path)
                
                self._repair_table(db, table, self.athena_output_s3_path)
                
        else:
            # Write the DataFrame to the table location
            if overwrite:
                # Delete the data in the S3 location
                files = S3Utils.get_keys_from_prefix(self.s3_client, bucket, prefix)
                for file in files:
                    self.s3_client.delete_object(Bucket=bucket, Key=file)
            
            s3_path = self._create_s3_path(bucket, prefix)
            writer = self.OUTPUT_FORMAT_TO_WRITER.get(table['StorageDescriptor']['OutputFormat'])
            if writer is None:
                raise ValueError(f'The table have a unsupported output format: {table["StorageDescriptor"]["OutputFormat"]}')
            writer(df, s3_path)
            
        
        
        
        
        
        
        