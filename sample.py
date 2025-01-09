"""
Demo of main features of dataengtools module for data manipulation in AWS Glue.

Required dependencies:
- boto3: AWS SDK for Python
- polars: Data analysis library
- dataengtools: Internal module for data engineering operations
- numpy: Numerical computing library

Installation:
pip install boto3 polars numpy
"""

import boto3
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from dataengtools.engine_factory import EngineFactory
from dataengtools.utils.logger import Logger

LOGGER = Logger.get_instance()

def create_example_table(database: str, table: str, location: str) -> None:
    """Creates an external table in AWS Glue for Parquet files."""
    
    # Define column schema
    columns = [
        {'Name': 'day', 'Type': 'int'},
        {'Name': 'hour', 'Type': 'int'},
        {'Name': 'minute', 'Type': 'int'},
        {'Name': 'second', 'Type': 'int'},
        {'Name': 'timestamp', 'Type': 'timestamp'},
        {'Name': 'value', 'Type': 'double'}
    ]
    
    # Define partition columns
    partitions = [
        {'Name': 'year', 'Type': 'int'},
        {'Name': 'month', 'Type': 'int'}
    ]
    
    # Check if table exists
    glue = boto3.client('glue')
    try:
        glue.get_table(DatabaseName=database, Name=table)
        LOGGER.info(f"Table {table} already exists.")
        return
    except glue.exceptions.EntityNotFoundException:
        pass

    # Create table
    table_input = {
        'Name': table,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {}
            }
        },
        'PartitionKeys': partitions,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {'EXTERNAL': 'TRUE'}
    }
    
    glue.create_table(DatabaseName=database, TableInput=table_input)
    LOGGER.info(f"Table {table} created successfully.")

def generate_sample_data(num_records: int = 1000) -> pl.DataFrame:
    """Generates a DataFrame with sample data."""
    base_date = datetime(2020, 1, 1)
    
    # Generate sequential timestamps
    df = pl.DataFrame({
        'timestamp': [base_date + timedelta(seconds=x) for x in range(num_records)],
    })
    
    # Add derived columns
    df = df.with_columns([
        pl.col('timestamp').dt.day().alias('day'),
        pl.col('timestamp').dt.hour().alias('hour'),
        pl.col('timestamp').dt.minute().alias('minute'),
        pl.col('timestamp').dt.second().alias('second'),
        pl.col('timestamp'),
        pl.Series(name='value', values=np.random.uniform(0, 100, num_records)),
        pl.col('timestamp').dt.year().alias('year'),
        pl.col('timestamp').dt.month().alias('month')
    ])
    
    return df

def main():
    # Configuration
    DATABASE = 'bronze-database-example'
    TABLE = 'partitioned_data'
    LOCATION = 's3://datalake-example/partitioned_data/'
    
    # 1. Create table in Glue
    create_example_table(DATABASE, TABLE, LOCATION)
    
    # 2. Generate sample data
    df = generate_sample_data(1000)
    LOGGER.info(f'DataFrame created with {len(df)} records')
    
    # 3. Demonstrate catalog operations
    catalog = EngineFactory().get_catalog_engine('dataframe|aws')
    
    # Get metadata
    location = catalog.get_location(DATABASE, TABLE)
    metadata = catalog.get_table_metadata(DATABASE, TABLE)
    partitions = catalog.get_partitions_columns(DATABASE, TABLE)
    
    LOGGER.info(f'Location: {location}')
    LOGGER.info(f'Partition columns: {partitions}')
    
    # 4. Write and read data
    df = catalog.adapt_frame_to_table_schema(df, DATABASE, TABLE)
    catalog.write_table(df, DATABASE, TABLE, True)
    
    # Read all data
    df_complete = catalog.read_table(DATABASE, TABLE)
    LOGGER.info(f'Data read: {len(df_complete)} records')
    
    # Read partitioned data
    df_filtered = catalog.read_partitioned_table(
        DATABASE, TABLE, 
        conditions='year = 2020 and month = 1'
    )
    LOGGER.info(f'Filtered data: {len(df_filtered)} records')

if __name__ == '__main__':
    main()