from typing import Generator, Dict
import pytest
from moto import mock_aws
import boto3
from mypy_boto3_glue import GlueClient
from mypy_boto3_s3 import S3Client
from dataengtools.aws.partition_handler import AWSGluePartitionHandler

@pytest.fixture
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Mocked AWS Credentials for moto.
    
    Args:
        monkeypatch: pytest fixture for modifying the environment
    """
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_SECURITY_TOKEN', 'testing')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'testing')

@pytest.fixture
def glue_client(aws_credentials) -> Generator[GlueClient, None, None]:
    """Create a mocked Glue client."""
    with mock_aws():
        client: GlueClient = boto3.client('glue')
        yield client

@pytest.fixture
def s3_client(aws_credentials) -> Generator[S3Client, None, None]:
    """Create a mocked S3 client."""
    with mock_aws():
        client: S3Client = boto3.client('s3')
        yield client

@pytest.fixture
def glue_handler(glue_client: GlueClient, s3_client: S3Client) -> AWSGluePartitionHandler:
    """Create an instance of AWSGluePartitionHandler."""
    return AWSGluePartitionHandler(glue_client, s3_client)

@pytest.fixture
def test_database(glue_client: GlueClient) -> str:
    """Create a test database in Glue."""
    database_name = "test_database"
    glue_client.create_database(
        DatabaseInput={
            'Name': database_name
        }
    )
    return database_name

@pytest.fixture
def test_table(glue_client: GlueClient, test_database: str, s3_client: S3Client) -> Dict[str, str]:
    """Create a test table with partitions in Glue."""
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    
    table_name = "test_table"
    table_location = f"s3://{bucket_name}/test_table/"
    
    glue_client.create_table(
        DatabaseName=test_database,
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Location': table_location,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                },
                'Columns': [
                    {'Name': 'col1', 'Type': 'string'}
                ]
            },
            'PartitionKeys': [
                {'Name': 'dt', 'Type': 'string'},
                {'Name': 'region', 'Type': 'string'}
            ]
        }
    )
    
    return {
        'database': test_database,
        'table': table_name,
        'location': table_location,
        'bucket': bucket_name
    }

class TestAWSGluePartitionHandler:

    def test_get_partitions_empty(
        self,
        glue_handler: AWSGluePartitionHandler,
        test_table: Dict[str, str]
    ) -> None:
        """Test getting partitions when there are none."""
        partitions = glue_handler.get_partitions(
            test_table['database'],
            test_table['table']
        )
        assert len(partitions) == 0

    def test_get_partitions_with_data(
        self,
        glue_client: GlueClient,
        glue_handler: AWSGluePartitionHandler,
        test_table: Dict[str, str]
    ) -> None:
        """Test getting partitions with existing data."""
        # Create test partitions
        glue_client.batch_create_partition(
            DatabaseName=test_table['database'],
            TableName=test_table['table'],
            PartitionInputList=[
                {
                    'Values': ['2024-01-01', 'US'],
                    'StorageDescriptor': {
                        'Location': f"{test_table['location']}dt=2024-01-01/region=US",
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                        }
                    }
                }
            ]
        )

        partitions = glue_handler.get_partitions(
            test_table['database'],
            test_table['table']
        )
        
        assert len(partitions) == 1
        assert partitions[0].values == ['2024-01-01', 'US']
        assert partitions[0].name == 'dt=2024-01-01/region=US'

    def test_delete_partitions(
        self,
        glue_client: GlueClient,
        glue_handler: AWSGluePartitionHandler,
        test_table: Dict[str, str]
    ) -> None:
        """Test deleting partitions."""
        # Create test partitions
        partition_values = [
            ['2024-01-01', 'US'],
            ['2024-01-01', 'EU']
        ]
        
        for values in partition_values:
            glue_client.batch_create_partition(
                DatabaseName=test_table['database'],
                TableName=test_table['table'],
                PartitionInputList=[{
                    'Values': values,
                    'StorageDescriptor': {
                        'Location': f"{test_table['location']}dt={values[0]}/region={values[1]}",
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                        }
                    }
                }]
            )

        # Get partitions and delete them
        partitions = glue_handler.get_partitions(
            test_table['database'],
            test_table['table']
        )
        
        assert len(partitions) == 2
        
        glue_handler.delete_partitions(
            test_table['database'],
            test_table['table'],
            partitions
        )
        
        # Verify partitions were deleted
        remaining_partitions = glue_handler.get_partitions(
            test_table['database'],
            test_table['table']
        )
        assert len(remaining_partitions) == 0

    def test_repair_table(
        self,
        glue_handler: AWSGluePartitionHandler,
        test_table: Dict[str, str],
        s3_client: S3Client
    ) -> None:
        """Test repairing table partitions."""
        # Create files in S3 that should become partitions
        s3_client.put_object(
            Bucket=test_table['bucket'],
            Key='test_table/dt=2024-01-01/region=US/data.csv',
            Body='test data'
        )
        s3_client.put_object(
            Bucket=test_table['bucket'],
            Key='test_table/dt=2024-01-01/region=EU/data.csv',
            Body='test data'
        )

        # Repair table
        glue_handler.repair_table(
            test_table['database'],
            test_table['table']
        )

        # Verify partitions were created
        partitions = glue_handler.get_partitions(
            test_table['database'],
            test_table['table']
        )
        
        assert len(partitions) == 2
        partition_values = {tuple(p.values) for p in partitions}
        assert ('2024-01-01', 'US') in partition_values
        assert ('2024-01-01', 'EU') in partition_values

    def test_repair_table_removes_missing_partitions(
        self,
        glue_client: GlueClient,
        glue_handler: AWSGluePartitionHandler,
        test_table: Dict[str, str],
        s3_client: S3Client
    ) -> None:
        """Test that repair table removes partitions that don't exist in S3."""
        # Create a partition in Glue that doesn't exist in S3
        glue_client.batch_create_partition(
            DatabaseName=test_table['database'],
            TableName=test_table['table'],
            PartitionInputList=[{
                'Values': ['2024-01-01', 'ASIA'],
                'StorageDescriptor': {
                    'Location': f"{test_table['location']}dt=2024-01-01/region=ASIA",
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                    }
                }
            }]
        )

        # Create a file in S3 for a different partition
        s3_client.put_object(
            Bucket=test_table['bucket'],
            Key='test_table/dt=2024-01-01/region=US/data.csv',
            Body='test data'
        )

        # Repair table
        glue_handler.repair_table(
            test_table['database'],
            test_table['table']
        )

        # Verify only the S3-backed partition exists
        partitions = glue_handler.get_partitions(
            test_table['database'],
            test_table['table']
        )
        
        assert len(partitions) == 1
        assert partitions[0].values == ['2024-01-01', 'US']