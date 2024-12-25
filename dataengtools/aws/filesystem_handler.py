
from io import TextIOWrapper
from typing import List
from mypy_boto3_s3 import S3Client 
from dataengtools.interfaces.filesystem import FilesystemHandler
from s3fs import S3FileSystem

class AWSS3FilesystemHandler(FilesystemHandler):
    """
    Implementation of FilesystemHandler for AWS S3.
    """
    
    def __init__(self, s3: S3Client, fs: S3FileSystem) -> None:
        self.s3 = s3
        self.fs = fs
    
    def get_filepaths(self, root: str, prefix: str) -> List[str]:
        """
        Retrieve a list of files from the filesystem.
        
        :param root: The root path of the filesystem.
        :param prefix: The prefix to filter files.
        :return: List of file paths.
        """
        
        paginator = self.s3.get_paginator('list_objects_v2')
        files = []
        
        for page in paginator.paginate(Bucket=root, Prefix=prefix):
            for obj in page.get('Contents', []):
                files.append(obj['Key'])
                
        return files
    
    def delete_files(self, root: str, files: List[str]) -> None:
        """
        Delete specified files from the filesystem.
        
        :param root: The root path of the filesystem.
        :param files: List of file paths to be deleted.
        """
        
        CHUNK_SIZE = 1000
        for i in range(0, len(files), CHUNK_SIZE):
            batch = files[i:i+CHUNK_SIZE]
            objects = [{'Key': key} for key in batch]
            
            self.s3.delete_objects(Bucket=root, Delete={'Objects': objects})

    def open_file(self, path: str, mode: str) -> TextIOWrapper:
        return self.fs.open(path, mode)