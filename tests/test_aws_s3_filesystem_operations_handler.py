from typing import Generator, List
import pytest
from moto import mock_aws
import boto3
from mypy_boto3_s3 import S3Client

from dataengtools.aws.filesystem_handler import AWSS3FilesystemOperationsHandler

@pytest.fixture
def s3_client() -> Generator[S3Client, None, None]:
    """
    Fixture to create a mocked S3 client.
    
    Returns:
        Generator[S3Client, None, None]: A mocked S3 client instance
    """
    with mock_aws():
        client: S3Client = boto3.client('s3')
        yield client

@pytest.fixture
def s3_handler(s3_client: S3Client) -> AWSS3FilesystemOperationsHandler:
    """
    Fixture to create an instance of AWSS3FilesystemOperationsHandler.
    
    Args:
        s3_client: The mocked S3 client
        
    Returns:
        AWSS3FilesystemOperationsHandler: An instance of the handler
    """
    return AWSS3FilesystemOperationsHandler(s3_client)

@pytest.fixture
def test_bucket(s3_client: S3Client) -> str:
    """
    Fixture to create and populate a test bucket.
    
    Args:
        s3_client: The mocked S3 client
        
    Returns:
        str: Name of the created test bucket
    """
    bucket_name: str = 'test-bucket'
    s3_client.create_bucket(Bucket=bucket_name)
    
    test_files: List[str] = [
        'prefix1/file1.txt',
        'prefix1/file2.txt',
        'prefix1/subfolder/file3.txt',
        'prefix2/file4.txt'
    ]
    
    for file_key in test_files:
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body='test content')
    
    return bucket_name

class TestAWSS3FilesystemOperationsHandler:
    
    def test_get_filepaths_with_prefix(
        self, 
        s3_handler: AWSS3FilesystemOperationsHandler, 
        test_bucket: str
    ) -> None:
        """
        Test getting files with a specific prefix.
        
        Args:
            s3_handler: The S3 operations handler
            test_bucket: Name of the test bucket
        """
        files: List[str] = s3_handler.get_filepaths(test_bucket, 'prefix1/')
        
        assert len(files) == 3
        assert 'prefix1/file1.txt' in files
        assert 'prefix1/file2.txt' in files
        assert 'prefix1/subfolder/file3.txt' in files
        assert 'prefix2/file4.txt' not in files

    def test_get_filepaths_empty_prefix(
        self, 
        s3_handler: AWSS3FilesystemOperationsHandler, 
        test_bucket: str
    ) -> None:
        """
        Test getting all files with empty prefix.
        
        Args:
            s3_handler: The S3 operations handler
            test_bucket: Name of the test bucket
        """
        files: List[str] = s3_handler.get_filepaths(test_bucket, '')
        
        assert len(files) == 4
        
    def test_get_filepaths_non_existent_prefix(
        self, 
        s3_handler: AWSS3FilesystemOperationsHandler, 
        test_bucket: str
    ) -> None:
        """
        Test getting files with a prefix that doesn't exist.
        
        Args:
            s3_handler: The S3 operations handler
            test_bucket: Name of the test bucket
        """
        files: List[str] = s3_handler.get_filepaths(test_bucket, 'nonexistent/')
        
        assert len(files) == 0

    def test_delete_files_single_file(
        self, 
        s3_handler: AWSS3FilesystemOperationsHandler, 
        test_bucket: str, 
        s3_client: S3Client
    ) -> None:
        """
        Test deleting a single file.
        
        Args:
            s3_handler: The S3 operations handler
            test_bucket: Name of the test bucket
            s3_client: The S3 client instance
        """
        files_to_delete: List[str] = ['prefix1/file1.txt']
        
        s3_handler.delete_files(test_bucket, files_to_delete)
        
        remaining_files: List[str] = s3_handler.get_filepaths(test_bucket, '')
        assert 'prefix1/file1.txt' not in remaining_files
        assert len(remaining_files) == 3

    def test_delete_files_multiple_files(
        self, 
        s3_handler: AWSS3FilesystemOperationsHandler, 
        test_bucket: str
    ) -> None:
        """
        Test deleting multiple files.
        
        Args:
            s3_handler: The S3 operations handler
            test_bucket: Name of the test bucket
        """
        files_to_delete: List[str] = ['prefix1/file1.txt', 'prefix1/file2.txt']
        
        s3_handler.delete_files(test_bucket, files_to_delete)
        
        remaining_files: List[str] = s3_handler.get_filepaths(test_bucket, '')
        assert 'prefix1/file1.txt' not in remaining_files
        assert 'prefix1/file2.txt' not in remaining_files
        assert len(remaining_files) == 2

    def test_delete_files_large_batch(
        self, 
        s3_handler: AWSS3FilesystemOperationsHandler, 
        test_bucket: str, 
        s3_client: S3Client
    ) -> None:
        """
        Test deleting more than 1000 files (chunk size).
        
        Args:
            s3_handler: The S3 operations handler
            test_bucket: Name of the test bucket
            s3_client: The S3 client instance
        """
        large_file_set: List[str] = [f'large/file{i}.txt' for i in range(1500)]
        for file_key in large_file_set:
            s3_client.put_object(Bucket=test_bucket, Key=file_key, Body='test content')
        
        s3_handler.delete_files(test_bucket, large_file_set)
        
        remaining_files: List[str] = s3_handler.get_filepaths(test_bucket, 'large/')
        assert len(remaining_files) == 0

    def test_delete_files_non_existent(
        self, 
        s3_handler: AWSS3FilesystemOperationsHandler, 
        test_bucket: str
    ) -> None:
        """
        Test deleting files that don't exist.
        
        Args:
            s3_handler: The S3 operations handler
            test_bucket: Name of the test bucket
        """
        non_existent_files: List[str] = ['nonexistent1.txt', 'nonexistent2.txt']
        
        # Should not raise an exception
        s3_handler.delete_files(test_bucket, non_existent_files)

    def test_delete_files_empty_list(
        self, 
        s3_handler: AWSS3FilesystemOperationsHandler, 
        test_bucket: str
    ) -> None:
        """
        Test deleting an empty list of files.
        
        Args:
            s3_handler: The S3 operations handler
            test_bucket: Name of the test bucket
        """
        # Should not raise an exception
        s3_handler.delete_files(test_bucket, [])
        
        remaining_files: List[str] = s3_handler.get_filepaths(test_bucket, '')
        assert len(remaining_files) == 4