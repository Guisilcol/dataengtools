import os
from typing import List, Set, Generator, Dict, Any
import pytest
import boto3
from moto import mock_aws
from mypy_boto3_glue import GlueClient
from mypy_boto3_s3 import S3Client
from dataengtools.core.interfaces.integration_layer.catalog_partitions import Partition
from dataengtools.providers.aws.glue_catalog_partitions_handler import AWSGluePartitionHandler

@pytest.fixture(scope='function')
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope='function')
def aws_mock() -> Generator[None, None, None]:
    """Setup mock for AWS services."""
    with mock_aws():
        yield

@pytest.fixture(scope='function')
def glue_client(aws_credentials: None, aws_mock: None) -> GlueClient:
    """Create mocked Glue client."""
    return boto3.client('glue')

@pytest.fixture(scope='function')
def s3_client(aws_credentials: None, aws_mock: None) -> S3Client:
    """Create mocked S3 client."""
    return boto3.client('s3')

@pytest.fixture(scope='function')
def glue_handler(glue_client: GlueClient, s3_client: S3Client) -> AWSGluePartitionHandler:
    """Create instance of AWSGluePartitionHandler with mocked clients."""
    return AWSGluePartitionHandler(glue_client, s3_client)

@pytest.fixture(scope='function')
def setup_test_environment(glue_client: GlueClient, s3_client: S3Client) -> None:
    """Setup test database, table and S3 bucket with test data."""
    # Create test database
    glue_client.create_database(
        DatabaseInput={
            'Name': 'test_database'
        }
    )

    # Create test table
    glue_client.create_table(
        DatabaseName='test_database',
        TableInput={
            'Name': 'test_table',
            'StorageDescriptor': {
                'Location': 's3://test-bucket/test_database/test_table',
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                }
            },
            'PartitionKeys': [
                {'Name': 'year', 'Type': 'string'},
                {'Name': 'month', 'Type': 'string'}
            ]
        }
    )

    # Create S3 bucket and test objects
    s3_client.create_bucket(Bucket='test-bucket')
    
    # Create some test files in S3
    test_files: List[str] = [
        'test_database/test_table/year=2024/month=01/file1.parquet',
        'test_database/test_table/year=2024/month=02/file1.parquet',
        'test_database/test_table/year=2024/month=03/file1.parquet'
    ]
    
    for file_path in test_files:
        s3_client.put_object(
            Bucket='test-bucket',
            Key=file_path,
            Body=b'test data'
        )

    # Create some test partitions in Glue
    for partition in [('2024', '01'), ('2024', '02'), ('2024', '04')]:
        glue_client.create_partition(
            DatabaseName='test_database',
            TableName='test_table',
            PartitionInput={
                'Values': list(partition),
                'StorageDescriptor': {
                    'Location': f's3://test-bucket/test_database/test_table/year={partition[0]}/month={partition[1]}',
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                }
            }
        )

class TestAWSGluePartitionHandler:
    
    def test_get_partitions_no_conditions(
        self,
        glue_handler: AWSGluePartitionHandler,
        setup_test_environment: None
    ) -> None:
        """Test getting all partitions without conditions."""
        partitions: List[Partition] = glue_handler.get_partitions('test_database', 'test_table')
        
        # Should return 3 partitions
        assert len(partitions) == 3
        
        # Check partition names
        partition_names: Set[str] = {p for p in partitions}
        expected_names: Set[str] = {
            'year=2024/month=01',
            'year=2024/month=02',
            'year=2024/month=04'
        }
        assert partition_names == expected_names

    def test_get_partitions_with_conditions(
        self,
        glue_handler: AWSGluePartitionHandler,
        setup_test_environment: None
    ) -> None:
        """Test getting partitions with a filter condition."""
        conditions: str = "year='2024' AND month='01'"
        partitions: List[Partition] = glue_handler.get_partitions('test_database', 'test_table', conditions)
        
        assert len(partitions) == 1
        assert partitions[0] == 'year=2024/month=01'

    def test_delete_partitions(
        self,
        glue_handler: AWSGluePartitionHandler,
        setup_test_environment: None
    ) -> None:
        """Test deleting multiple partitions."""
        # Create partitions to delete
        partitions_to_delete: List[Partition] = [
            Partition('year=2024/month=01'),
            Partition('year=2024/month=02')
        ]
        
        # Delete partitions
        glue_handler.delete_partitions('test_database', 'test_table', partitions_to_delete)
        
        # Verify partitions were deleted
        remaining_partitions: List[Partition] = glue_handler.get_partitions('test_database', 'test_table')
        assert len(remaining_partitions) == 1
        assert remaining_partitions[0] == 'year=2024/month=04'

    def test_list_s3_partitions(
        self,
        glue_handler: AWSGluePartitionHandler,
        setup_test_environment: None
    ) -> None:
        """Test listing partitions from S3."""
        partitions: List[str] = glue_handler._list_s3_partitions(
            'test-bucket',
            'test_database/test_table'
        )
        
        expected_partitions: Set[str] = {
            'year=2024/month=01',
            'year=2024/month=02',
            'year=2024/month=03'
        }
        
        assert set(partitions) == expected_partitions

    def test_repair_table(
        self,
        glue_handler: AWSGluePartitionHandler,
        setup_test_environment: None
    ) -> None:
        """Test full table repair process."""
        # Initial state:
        # - S3 has: 01, 02, 03
        # - Glue has: 01, 02, 04
        # After repair:
        # - Should create: 03
        # - Should delete: 04
        
        glue_handler.repair_table('test_database', 'test_table')
        
        # Verify final state
        final_partitions: List[Partition] = glue_handler.get_partitions('test_database', 'test_table')
        final_partition_names: Set[str] = {p for p in final_partitions}
        
        expected_names: Set[str] = {
            'year=2024/month=01',
            'year=2024/month=02',
            'year=2024/month=03'
        }
        
        assert final_partition_names == expected_names

    def test_repair_table_empty_s3(
        self,
        glue_handler: AWSGluePartitionHandler,
        setup_test_environment: None,
        s3_client: S3Client
    ) -> None:
        """Test repair when S3 is empty."""
        # Delete all S3 objects
        objects: Dict[str, Any] = s3_client.list_objects_v2(Bucket='test-bucket')
        for obj in objects.get('Contents', []):
            s3_client.delete_object(Bucket='test-bucket', Key=obj['Key'])
        
        # Repair should remove all partitions
        glue_handler.repair_table('test_database', 'test_table')
        
        final_partitions: List[Partition] = glue_handler.get_partitions('test_database', 'test_table')
        assert len(final_partitions) == 0

    def test_repair_table_no_existing_partitions(
        self,
        glue_handler: AWSGluePartitionHandler,
        setup_test_environment: None
    ) -> None:
        """Test repair when Glue has no partitions."""
        # Delete all existing partitions
        partitions: List[Partition] = glue_handler.get_partitions('test_database', 'test_table')
        glue_handler.delete_partitions('test_database', 'test_table', partitions)
        
        # Repair should create partitions for all S3 paths
        glue_handler.repair_table('test_database', 'test_table')
        
        final_partitions: List[Partition] = glue_handler.get_partitions('test_database', 'test_table')
        assert len(final_partitions) == 3

    @pytest.mark.parametrize("database,table", [
        ("non_existent_db", "test_table"),
        ("test_database", "non_existent_table")
    ])
    def test_repair_table_invalid_inputs(
        self,
        glue_handler: AWSGluePartitionHandler,
        setup_test_environment: None,
        database: str,
        table: str
    ) -> None:
        """Test repair with invalid database or table names."""
        with pytest.raises(Exception):
            glue_handler.repair_table(database, table)