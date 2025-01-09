from typing import List, Optional, Dict
from mypy_boto3_glue import GlueClient
from mypy_boto3_s3 import S3Client
from dataengtools.core.interfaces.integration_layer.catalog_partitions import Partition, PartitionHandler
from dataengtools.utils.partition_helper import PartitionHelper
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class AWSGluePartitionHandler(PartitionHandler):
    def __init__(self, glue: GlueClient, s3: S3Client) -> None:
        """
        Initialize the AWS Glue Partition Handler.

        Args:
            glue: AWS Glue client instance
            s3: AWS S3 client instance
        """
        self.glue = glue
        self.s3 = s3

    def get_partitions(self, database: str, table: str, conditions: Optional[str] = None) -> List[Partition]:
        """
        Retrieve all partitions from a Glue table.

        Args:
            database: Glue database name
            table: Glue table name
            conditions: Optional filter conditions

        Returns:
            List of Partition objects
            
        Example:
            >>> handler.get_partitions("my_database", "my_table")
            [
                Partition("year=2024/month=01"),
                Partition("year=2024/month=02")
            ]
        """
        paginator = self.glue.get_paginator('get_partitions')
        partitions = []
        
        table_response = self.glue.get_table(DatabaseName=database, Name=table)
        base_location = table_response['Table']['StorageDescriptor']['Location'].rstrip('/')
        
        if conditions is None:
            pages = paginator.paginate(DatabaseName=database, TableName=table)
        else:
            pages = paginator.paginate(DatabaseName=database, TableName=table, Expression=conditions)
        
        raw_partitions = [p for page in pages for p in page.get('Partitions', [])]

        partitions = []
        for partition in raw_partitions:
            location = partition['StorageDescriptor']['Location'].rstrip('/')

            # Remove the table base location prefix to get partition name
            if location.startswith(base_location):
                name = location.replace(base_location + '/', '').strip('/')
            else:
                name = location

            partitions.append(Partition(name))

        return partitions

    def delete_partitions(self, database: str, table: str, partitions: List[Partition]) -> None:
        """
        Delete multiple partitions from a Glue table in batches.

        Args:
            database: Glue database name
            table: Glue table name
            partitions: List of partitions to delete

        Example:
            >>> partitions_to_delete = [
            ...     Partition("year=2024/month=01"),
            ...     Partition("year=2024/month=02")
            ... ]
            >>> handler.delete_partitions("my_database", "my_table", partitions_to_delete)
        """
        CHUNK_SIZE = 25
        for i in range(0, len(partitions), CHUNK_SIZE):
            batch = partitions[i:i+CHUNK_SIZE]
            batch_values = [PartitionHelper.get_values_from_partition(p) for p in batch]
            partitions_to_delete = [{'Values': p} for p in batch_values]

            self.glue.batch_delete_partition(
                DatabaseName=database,
                TableName=table,
                PartitionsToDelete=partitions_to_delete
            )

    def _create_partitions_batch(self, database: str, table: str, partition_info_list: List[Dict], storage_descriptor: Dict) -> None:
        """
        Create multiple partitions in the Glue table using batch operations.

        Args:
            database: Glue database name
            table: Glue table name
            partition_info_list: List of partition information dictionaries
            storage_descriptor: Storage descriptor from parent table

        Example:
            >>> partition_info = [
            ...     {
            ...         'values': ['2024', '01'],
            ...         'location': 's3://bucket/path/year=2024/month=01'
            ...     }
            ... ]
            >>> handler._create_partitions_batch("my_database", "my_table", 
            ...                                 partition_info, storage_descriptor)
        """
        CHUNK_SIZE = 100
        try:
            for i in range(0, len(partition_info_list), CHUNK_SIZE):
                batch = partition_info_list[i:i + CHUNK_SIZE]
                
                partition_inputs = []
                for info in batch:
                    partition_input = {
                        'Values': info['values'],
                        'StorageDescriptor': {
                            **storage_descriptor,
                            'Location': info['location']
                        }
                    }
                    partition_inputs.append(partition_input)
                
                self.glue.batch_create_partition(
                    DatabaseName=database,
                    TableName=table,
                    PartitionInputList=partition_inputs
                )
                
                logger.info(f"Successfully created batch of {len(batch)} partitions")
                
        except Exception as e:
            logger.error(f"Error creating partition batch: {e}")
            raise

    def _list_s3_partitions(self, bucket: str, prefix: str) -> List[str]:
        """
        List all partition directories that have data in S3.

        Args:
            bucket: S3 bucket name
            prefix: S3 prefix path (e.g., 'database/table')

        Returns:
            List of partition directories without bucket and table prefix

        Example:
            >>> handler._list_s3_partitions("my-bucket", "data/table1")
            [
                "year=2024/month=01",
                "year=2024/month=02"
            ]
        """
        try:
            s3_partitions = set()
            paginator = self.s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Extract only the partition directory
                        path_parts = obj['Key'].split('/')
                        # Remove the file name and any empty strings
                        path_parts = [p for p in path_parts[:-1] if p]
                        
                        # Skip if we don't have more parts than the prefix
                        prefix_parts = [p for p in prefix.split('/') if p]
                        if len(path_parts) <= len(prefix_parts):
                            continue
                            
                        # Get only the partition part of the path
                        partition_path = '/'.join(path_parts[len(prefix_parts):])
                        s3_partitions.add(partition_path)
            
            return list(s3_partitions)
            
        except Exception as e:
            logger.error(f"Error listing S3 objects: {e}")
            raise

    def repair_table(self, database: str, table: str) -> None:
        """
        Repair Glue table partitions by removing partitions without data and creating new ones.
        
        Args:
            database: Glue database name
            table: Glue table name

        Example:
            >>> handler.repair_table("my_database", "my_table")
            # This will:
            # 1. Remove partitions that don't have data in S3
            # 2. Create new partitions for S3 paths that aren't in the Glue catalog
            # Logs will show:
            # "Deleting 5 partitions"
            # "Creating 3 new partitions"
            # "Table my_database.my_table repair completed successfully"
        """
        try:
            # Get table information
            table_info = self.glue.get_table(DatabaseName=database, Name=table)
            base_location = table_info['Table']['StorageDescriptor']['Location'].rstrip('/')
            storage_descriptor = table_info['Table']['StorageDescriptor']
            
            # Parse S3 location
            parsed_url = urlparse(base_location)
            bucket = parsed_url.netloc
            prefix = parsed_url.path.lstrip('/')
            
            # List existing Glue partitions
            existing_partitions = self.get_partitions(database, table)
            existing_locations = {p for p in existing_partitions}
            
            # List partitions with data in S3
            s3_partitions = self._list_s3_partitions(bucket, prefix)
            
            # Identify partitions to delete (exist in Glue but not in S3)
            partitions_to_delete = []
            for partition in existing_partitions:
                if partition not in s3_partitions:
                    partitions_to_delete.append(partition)
            
            # Delete partitions in batch
            if partitions_to_delete:
                logger.info(f"Deleting {len(partitions_to_delete)} partitions")
                self.delete_partitions(database, table, partitions_to_delete)
            
            # Prepare information for new partitions
            partitions_to_create = []
            for s3_path in s3_partitions:
                if s3_path not in existing_locations:
                    partition_values = PartitionHelper.get_values_from_partition(Partition(s3_path))
                    partitions_to_create.append({
                        'values': partition_values,
                        'location': f"{base_location}/{s3_path}"
                    })
            
            # Create new partitions in batch
            if partitions_to_create:
                logger.info(f"Creating {len(partitions_to_create)} partitions")
                self._create_partitions_batch(database, table, partitions_to_create, storage_descriptor)
            
            logger.info(f"Table {database}.{table} repair completed successfully")
            
        except Exception as e:
            logger.error(f"Error during table {database}.{table} repair: {e}")
            raise