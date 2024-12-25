from typing import Generator
import pytest
from moto import mock_aws
import boto3
from mypy_boto3_glue import GlueClient
from dataengtools.aws.table_metadata_retriver import AWSGlueTableMetadataRetriver
from dataengtools.interfaces.metadata import Column

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
def metadata_retriever(glue_client: GlueClient) -> AWSGlueTableMetadataRetriver:
    """Create an instance of AWSGlueTableMetadataRetriver."""
    return AWSGlueTableMetadataRetriver(glue_client)

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
def test_table(glue_client: GlueClient, test_database: str) -> str:
    """Create a test table in Glue."""
    table_name = "test_table"
    glue_client.create_table(
        DatabaseName=test_database,
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Location': f"s3://test-bucket/test_table/",
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                    'Parameters': {
                        'field.delim': ',',
                        'skip.header.line.count': '1'
                    }
                },
                'Columns': [
                    {'Name': 'col1', 'Type': 'string'},
                    {'Name': 'col2', 'Type': 'int'}
                ]
            },
            'PartitionKeys': [
                {'Name': 'dt', 'Type': 'string'},
                {'Name': 'region', 'Type': 'string'}
            ]
        }
    )
    return table_name

class TestAWSGlueTableMetadataRetriver:

    def test_get_table_metadata_success(
        self,
        metadata_retriever: AWSGlueTableMetadataRetriver,
        glue_client: GlueClient,
        test_database: str,
        test_table: str
    ) -> None:
        """Test retrieving table metadata successfully."""
        metadata = metadata_retriever.get_table_metadata(test_database, test_table)

        columns = [Column(name='col1', datatype='string'), Column(name='col2', datatype='int')]
        partition_columns = [Column(name='dt', datatype='string'), Column(name='region', datatype='string')]
        all_columns = columns + partition_columns

        assert metadata.database == test_database
        assert metadata.table == test_table
        assert metadata.columns == columns
        assert metadata.partition_columns == partition_columns
        assert metadata.all_columns == all_columns
        assert metadata.location == "s3://test-bucket/test_table/"
        assert metadata.files_have_header is True
        assert metadata.files_extension == 'csv'
        assert metadata.columns_separator == ','
        assert metadata.source == 'AWS Glue'

    def test_get_table_metadata_table_not_found(
        self,
        metadata_retriever: AWSGlueTableMetadataRetriver,
        test_database: str
    ) -> None:
        """Test retrieving metadata for a non-existent table."""
        with pytest.raises(ValueError, match=f"Table non_existent_table not found in database {test_database}"):
            metadata_retriever.get_table_metadata(test_database, "non_existent_table")

    def test_get_table_metadata_unknown_input_format(
        self,
        metadata_retriever: AWSGlueTableMetadataRetriver,
        glue_client: GlueClient,
        test_database: str
    ) -> None:
        """Test retrieving table metadata with an unknown input format."""
        glue_client.create_table(
            DatabaseName=test_database,
            TableInput={
                'Name': 'unknown_format_table',
                'StorageDescriptor': {
                    'Location': "s3://test-bucket/unknown_table/",
                    'InputFormat': 'org.unknown.format.UnknownInputFormat',
                    'Columns': [
                        {'Name': 'col1', 'Type': 'string'}
                    ]
                }
            }
        )

        metadata = metadata_retriever.get_table_metadata(test_database, 'unknown_format_table')

        assert metadata.files_extension == 'unknown'

    def test_get_table_metadata_missing_serde_params(
        self,
        metadata_retriever: AWSGlueTableMetadataRetriver,
        glue_client: GlueClient,
        test_database: str
    ) -> None:
        """Test retrieving table metadata when serde parameters are missing."""
        glue_client.create_table(
            DatabaseName=test_database,
            TableInput={
                'Name': 'no_serde_table',
                'StorageDescriptor': {
                    'Location': "s3://test-bucket/no_serde_table/",
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'Columns': [
                        {'Name': 'col1', 'Type': 'string'}
                    ],
                    'SerdeInfo': {}
                }
            }
        )

        metadata = metadata_retriever.get_table_metadata(test_database, 'no_serde_table')

        assert metadata.columns_separator is None
        assert metadata.files_have_header is False
