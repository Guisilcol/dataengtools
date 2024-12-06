from typing import Tuple, List
from mypy_boto3_s3 import S3Client

class S3Utils:
    
    @staticmethod
    def get_bucket_and_prefix(s3_path: str) -> Tuple[str, str]:
        return s3_path.replace('s3://', '').split('/', 1)
    
    @staticmethod
    def get_keys_from_prefix(s3_client: S3Client, bucket: str, prefix: str) -> List[str]:
        paginator = s3_client.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        files = []
        for response in response_iterator:
            for obj in response['Contents']:
                files.append(obj['Key'])
                
        return files