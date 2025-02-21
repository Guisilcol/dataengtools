from typing import List, Optional, Dict
from mypy_boto3_glue import GlueClient
from mypy_boto3_s3 import S3Client
from dataengtools.core.interfaces.integration_layer.catalog_partitions import Partition, PartitionHandler
from dataengtools.utils.partition_helper import PartitionHelper
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class AWSGluePartitionHandler(PartitionHandler):
    """
    AWSGluePartitionHandler handles table partition operations in AWS Glue.

    This implementation provides methods to retrieve, delete, and repair
    partitions of a Hive-style table using AWS Glue and S3.
    """

    def __init__(self, glue: GlueClient, s3: S3Client) -> None:
        """
        Initialize the AWSGluePartitionHandler with AWS Glue and S3 clients.

        Parameters:
            glue (GlueClient): An AWS Glue client instance.
            s3 (S3Client): An AWS S3 client instance.
        """
        self.glue = glue
        self.s3 = s3

    def get_partitions(self, database: str, table: str, conditions: Optional[str] = None) -> List[Partition]:
        """
        Retrieve all partitions of a Glue table.

        This method uses AWS Glue's pagination to list partitions from
        the specified database and table. It processes the returned partition
        locations to extract partition names relative to the base storage location.

        Parameters:
            database (str): The name of the Glue database.
            table (str): The name of the Glue table.
            conditions (Optional[str]): Optional filter conditions to limit partitions.

        Returns:
            List[Partition]: A list of Partition objects representing each partition.

        Example:
            >>> handler.get_partitions("my_database", "my_table")
            [Partition("year=2024/month=01"), Partition("year=2024/month=02")]
        """
        paginator = self.glue.get_paginator('get_partitions')
        
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

        This method deletes partitions in chunks (of up to 25 partitions per batch)
        by preparing the required input for the Glue batch_delete_partition API.

        Parameters:
            database (str): The name of the Glue database.
            table (str): The name of the Glue table.
            partitions (List[Partition]): A list of partitions to be deleted.

        Returns:
            None

        Example:
            >>> partitions_to_delete = [Partition("year=2024/month=01"), Partition("year=2024/month=02")]
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
        Create multiple partitions in a Glue table using batch operations.

        This method processes a list of partition information dictionaries and creates
        partitions in chunks (of up to 100 partitions per batch) by calling the Glue API.

        Parameters:
            database (str): The name of the Glue database.
            table (str): The name of the Glue table.
            partition_info_list (List[Dict]): List of partition information dictionaries. 
                Each dictionary should contain 'values' and 'location' keys.
            storage_descriptor (Dict): The storage descriptor obtained from the parent table.

        Returns:
            None

        Example:
            >>> partition_info = [{
            ...     'values': ['2024', '01'],
            ...     'location': 's3://bucket/path/year=2024/month=01'
            ... }]
            >>> handler._create_partitions_batch("my_database", "my_table", partition_info, storage_descriptor)
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
        List all partition directories that contain data in S3.

        This method uses S3 pagination to list objects under the specified prefix and extracts
        the partition directories from the object keys.

        Parameters:
            bucket (str): The S3 bucket name.
            prefix (str): The S3 prefix path (e.g., 'database/table').

        Returns:
            List[str]: A list of partition directory strings (without bucket and table prefix).

        Example:
            >>> handler._list_s3_partitions("my-bucket", "data/table1")
            ["year=2024/month=01", "year=2024/month=02"]
        """
        try:
            s3_partitions = set()
            paginator = self.s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Extract partition directory parts from the S3 object key
                        path_parts = obj['Key'].split('/')
                        # Remove the file name and any empty strings
                        path_parts = [p for p in path_parts[:-1] if p]
                        
                        # Skip if the partition information is not present
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
        Repair a Glue table by synchronizing its partitions with the actual data in S3.

        This method performs the following steps:
            1. Retrieves the current table information and base S3 location.
            2. Lists existing Glue partitions and S3 directories that contain data.
            3. Identifies partitions that exist in Glue but not in S3, and deletes them.
            4. Identifies S3 partitions that are missing in Glue, and creates them in batch.

        Parameters:
            database (str): The name of the Glue database.
            table (str): The name of the Glue table.

        Returns:
            None

        Example:
            >>> handler.repair_table("my_database", "my_table")
            # This will remove stale partitions and create new ones based on S3 data.
        """
        try:
            # Get table information and base location
            table_info = self.glue.get_table(DatabaseName=database, Name=table)
            base_location = table_info['Table']['StorageDescriptor']['Location'].rstrip('/')
            storage_descriptor = table_info['Table']['StorageDescriptor']
            
            # Parse S3 location to get bucket and prefix
            parsed_url = urlparse(base_location)
            bucket = parsed_url.netloc
            prefix = parsed_url.path.lstrip('/')
            
            # List existing Glue partitions and S3 partitions
            existing_partitions = self.get_partitions(database, table)
            existing_locations = {p for p in existing_partitions}
            s3_partitions = self._list_s3_partitions(bucket, prefix)
            
            # Identify Glue partitions that do not have data in S3
            partitions_to_delete = []
            for partition in existing_partitions:
                if partition not in s3_partitions:
                    partitions_to_delete.append(partition)
            
            # Delete partitions that are stale in Glue
            if partitions_to_delete:
                logger.info(f"Deleting {len(partitions_to_delete)} partitions")
                self.delete_partitions(database, table, partitions_to_delete)
            
            # Prepare new partitions from S3 that are missing in Glue
            partitions_to_create = []
            for s3_path in s3_partitions:
                if s3_path not in existing_locations:
                    partition_values = PartitionHelper.get_values_from_partition(Partition(s3_path))
                    partitions_to_create.append({
                        'values': partition_values,
                        'location': f"{base_location}/{s3_path}"
                    })
            
            # Create missing partitions in Glue
            if partitions_to_create:
                logger.info(f"Creating {len(partitions_to_create)} partitions")
                self._create_partitions_batch(database, table, partitions_to_create, storage_descriptor)
            
            logger.info(f"Table {database}.{table} repair completed successfully")
            
        except Exception as e:
            logger.error(f"Error during table {database}.{table} repair: {e}")
            raise