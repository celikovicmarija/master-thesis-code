import json
import os
import sys
import threading

import boto3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from boto3.s3.transfer import TransferConfig

BUCKET_NAME = 'my-unique-bucket-name'


def s3_client():
    s3 = boto3.client('s3')
    ''' :type : pyboto3.s3 '''
    return s3


def s3_resource():
    s3 = boto3.resource('s3')
    return s3


def create_bucket(bucket_name):
    return s3_client().create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})


def create_bucket_policy():
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'AddPerm',
                'Effect': 'Allow',
                'Principal': '*',
                'Action': ['s3:*'],
                'Resource': [f'arn:aws:s3:::{BUCKET_NAME}/*']
            }
        ]
    }

    policy_json = json.dumps(bucket_policy)
    return s3_client().put_bucket_policy(
        Bucket=BUCKET_NAME,
        Policy=policy_json
    )


def list_buckets():
    return s3_client().list_buckets()


def get_bucket_policy():
    return s3_client().get_bucket_policy(BUCKET_NAME)


def get_bucket_encryption():
    return s3_client().get_bucket_encryption(BUCKET_NAME)


def update_bucket_policy(bucket_name):
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'AddPerm',
                'Effect': 'Allow',
                'Allow': '*',
                'Action': [
                    's3:DeleteObject',
                    's3:GetObject',
                    's3:PutObject'
                ],
                'Resource': f'arn:aws:s3:::{bucket_name}/*'
            }
        ]
    }
    policy_json = json.dumps(bucket_policy)
    return s3_client().put_bucket_policy(policy_json)


def server_side_encrypt_bucket():
    return s3_client().put_bucket_encryption(
        Bucket=BUCKET_NAME,
        ServerSideEncryptionConfiguration={
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES156'
                    }
                }
            ]

        }
    )


def delete_bucket():
    return s3_client().delete_bucket(Bucket=BUCKET_NAME)


def upload_small_file():
    file_path = os.path.dirname(__file__) + '/readme.txt'
    return s3_client().upload_file(
        Filename=file_path,
        Bucket=BUCKET_NAME,
        Key='readme.txt')


def upload_large_file():
    config = TransferConfig(
        multipart_threshold=1024 * 25,
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True
    )
    file_path = os.path.dirname(__file__) + '/largefile.pdf'
    key_path = 'multipart_files/largefile.pdf'
    s3_resource().meta.client.upload_file(file_path, BUCKET_NAME, key_path, ExtraArgs={'ACL': 'public-read',
                                                                                       'ContentType': 'text/pdf'},
                                          Config=config,
                                          Callback=ProgressPercentage(file_path))


class ProgressPercentage(object):
    def __int__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size, percentage
                )
            )
            sys.stdout.flush()


def read_object_from_bucket():
    object_key = 'readme.txt'
    return s3_client().get_object(
        Bucket=BUCKET_NAME,
        Key=object_key
    )


def version_bucket_files():
    s3_client().put_bucket_versioning(
        Bucket=BUCKET_NAME,
        VersioningConfiguration={
            'Status': 'Enabled'
        }
    )


def upload_new_version():
    file_path = os.path.dirname(__file__) + '/readme.txt'
    return s3_client().upload_file(file_path, BUCKET_NAME, 'readme.txt')


def put_lifecycle_policy():
    lifecycle_policy = {
        "Rules": [
            {
                "ID": "Move readme file to Glacier",
                "Prefix": "readme",
                "Status": "Enabled",
                "Transitions": [
                    {
                        "Date": "2019-01-01T00:00:00.000Z"
                    }
                ]
            },
            {
                "Status": "Enabled",
                "Prefix": "",
                "NoncurrentVersionTransitions": [
                    {
                        "NoncurrentDays": 2,
                        "StorageClass": "GLACIER"
                    }
                ],
                "ID": "Move old versions to Glacier"
            }
        ]
    }
    s3_client().put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME,
        LifecycleConfiguration=lifecycle_policy
    )



def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

if __name__ == '__main__':
    print('Hello')
    # print(upload_new_version())
    # print(version_bucket_files())
    # print(read_object_from_bucket())
    # print(upload_large_file())
    # print(upload_small_file())
    # print(delete_bucket())
    # print(server_side_encrypt_bucket())
    # print(update_bucket_policy())
    # print(get_bucket_encryption())
    # print(get_bucket_policy())
    # print(list_buckets())
    # print(create_bucket(BUCKET_NAME))
    # print(create_bucket_policy())
