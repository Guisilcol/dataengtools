from typing import List
from mypy_boto3_glue import GlueClient
from botocore.exceptions import ClientError
import polars as pl
from dataengtools.core.interfaces.integration_layer.catalog_metadata import (
    TableMetadataRetriver, TableMetadata, Column, DataTypeMapping, DatabaseMetadata, DatabaseMetadataRetriever
)


class AWSGlueDataTypeToPolars(DataTypeMapping[str, pl.DataType]):
    """
    Maps AWS Glue data type strings to Polars data types.

    This class defines a static mapping dictionary (MAPPING) that converts
    string representations of AWS Glue data types into corresponding Polars data types.
    """
    MAPPING = {
        'string': pl.Utf8,          # UTF-8 text
        'int': pl.Int64,            # 64-bit integer
        'bigint': pl.Int64,         # Adjusted to Int64 (64-bit integer)
        'double': pl.Float64,       # Adjusted to Float64
        'float': pl.Float32,        # Adjusted to Float32
        'boolean': pl.Boolean,      # Adjusted to Boolean
        'timestamp': pl.Datetime,   # Adjusted to Datetime
        'date': pl.Date,            # Adjusted to Date
        'decimal': pl.Float64,      # Represented as Float64
        'array': pl.List,           # Arrays mapped to List
        'map': pl.Object,           # Maps mapped to Object
        'struct': pl.Object,        # Structs mapped to Object
        'binary': pl.Binary         # Adjusted to Binary
    }


class AWSGlueTableMetadataRetriver(TableMetadataRetriver):
    """
    Retrieves table metadata from AWS Glue.

    This implementation uses the AWS Glue API to fetch table details, and then
    converts the raw table information into a standardized TableMetadata structure.
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
        """
        Initialize the metadata retriever with a GlueClient instance.

        Parameters:
            glue (GlueClient): A boto3 client for AWS Glue.
        """
        self.glue = glue
    
    def _create_table_metadata(self, table: dict) -> TableMetadata:
        """
        Convert raw AWS Glue table data into a standardized TableMetadata object.

        Parameters:
            table (dict): Raw table data as returned from the AWS Glue API.

        Returns:
            TableMetadata: An object containing structured metadata information.
        """
        columns = [
            Column(name=col['Name'], datatype=col['Type']) 
            for col in table['StorageDescriptor']['Columns']
        ]
        
        partition_columns = [
            Column(name=col['Name'], datatype=col['Type']) 
            for col in table.get('PartitionKeys', [])
        ]
        
        all_columns = columns + partition_columns
        location = table['StorageDescriptor']['Location']
        if location.endswith('/'):
            location = location[:-1]
        
        serde_params = table['StorageDescriptor'].get('SerdeInfo', {}).get('Parameters', {})
        
        columns_separator = serde_params.get('field.delim') or serde_params.get('separatorChar')
        files_have_header = serde_params.get('skip.header.line.count', '0') != '0'
        files_extension = self.INPUT_FORMAT_TO_FILE_TYPE.get(table['StorageDescriptor']['InputFormat'], 'unknown')
        
        raw_metadata = table
        source = 'AWS Glue'
        
        return TableMetadata(
            database=table['DatabaseName'],
            table=table['Name'],
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

    def get_all_tables(self, database: str, additional_configs: dict = {}) -> List[TableMetadata]:
        """
        Retrieve metadata for all tables in a specified database from AWS Glue.

        Parameters:
            database (str): The name of the database.
            additional_configs (dict, optional): Additional configuration options (default is empty dict).

        Returns:
            List[TableMetadata]: A list of TableMetadata objects for every table in the database.
        """
        paginator = self.glue.get_paginator('get_tables')
        pages = paginator.paginate(DatabaseName=database)
        
        tables = []
        for page in pages:
            for table in page['TableList']:
                tables.append(self._create_table_metadata(table))
        
        return tables

    def get_table_metadata(self, database: str, table: str) -> TableMetadata:
        """
        Retrieve metadata for a specific table in a database from AWS Glue.

        Parameters:
            database (str): The name of the database.
            table (str): The name of the table.

        Returns:
            TableMetadata: A TableMetadata object with detailed metadata about the table.

        Raises:
            ValueError: If the specified table is not found in the database.
            ClientError: Propagates any other client errors from AWS Glue.
        """
        try:
            response = self.glue.get_table(DatabaseName=database, Name=table)
            return self._create_table_metadata(response['Table'])
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                raise ValueError(f"Table {table} not found in database {database}") from e
            else:
                raise e
                

class AWSGlueDatabaseMetadataRetriever(DatabaseMetadataRetriever):
    """
    Retrieves database metadata from AWS Glue.

    This implementation uses the AWS Glue API to fetch information about databases,
    including the list of tables and raw metadata.
    """
    
    def __init__(self, glue: GlueClient) -> None:
        """
        Initialize the database metadata retriever with a GlueClient instance.

        Parameters:
            glue (GlueClient): A boto3 client for AWS Glue.
        """
        self.glue = glue
        
    def get_database_metadata(self, database: str) -> DatabaseMetadata:
        """
        Retrieve metadata for a specific database from AWS Glue.

        Parameters:
            database (str): The name of the database.

        Returns:
            DatabaseMetadata: An object containing metadata and table information for the database.
        """
        response = self.glue.get_database(Name=database)

        tables = self.glue.get_tables(DatabaseName=database)
        table_names = [table['Name'] for table in tables['TableList']]
        
        return DatabaseMetadata(
            name=database,
            tables=table_names,
            raw_metadata=response,
            source='AWS Glue'
        )  
    
    def get_all_databases(self) -> List[DatabaseMetadata]:
        """
        Retrieve metadata for all databases available in AWS Glue.

        Returns:
            List[DatabaseMetadata]: A list of DatabaseMetadata objects, each representing a database.
        """
        response = self.glue.get_databases()
        return [
            DatabaseMetadata(
                name=database['Name'],
                tables=[table['Name'] for table in self.glue.get_tables(DatabaseName=database['Name'])['TableList']],
                raw_metadata=database,
                source='AWS Glue'
            )
            for database in response['DatabaseList']
        ]