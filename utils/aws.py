import os
from logging import getLogger
from tempfile import TemporaryDirectory

import boto3
from botocore.exceptions import ClientError

from settings import AWS_REGION, LOCALSTACK_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from utils.parsing import read_dataframe_by_file_path

logger = getLogger(__name__)


def create_client(service_name: str) -> boto3.client:
    """
    Create an S3 client for the tenant.
    :param service_name: Name of the service to create a client for.
    :return: The S3 client.
    """
    return boto3.client(
        service_name,
        region_name=AWS_REGION,
        endpoint_url=LOCALSTACK_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )


def upload_file_to_bucket(
    bucket_name: str,
    file_path: str,
    key: str,
) -> None:
    """
    Upload a file to an S3 bucket.
    :param bucket_name: Name of the bucket to upload to.
    :param file_path: Path to the file to upload.
    :param key: Key to use for the file in the bucket.
    :return: None.
    """
    logger.info(f"Uploading file {file_path} to bucket {bucket_name}")
    client = create_client("s3")
    client.upload_file(file_path, bucket_name, key)


def list_s3_objects(s3_client: boto3.client, bucket_name: str, prefix: str):
    """
    List objects in an S3 bucket under a prefix
    @param s3_client: The AWS S3 client to use
    @param bucket_name: The name of the bucket to use
    @param prefix: The path to list under
    @return: A generator of all the objects in the requested path
    """
    next_token = ""
    list_object_args = {"Bucket": bucket_name, "Prefix": prefix}
    while next_token is not None:
        if next_token:
            list_object_args.update({"ContinuationToken": next_token})

        objects_in_bucket = s3_client.list_objects_v2(**list_object_args)
        for obj in objects_in_bucket.get("Contents", []):
            yield obj

        next_token = objects_in_bucket.get("NextContinuationToken")


def list_s3_files(s3_client: boto3.client, bucket_name: str, prefix: str):
    """
    List files in an S3 bucket under a prefix
    @param s3_client: The AWS S3 client to use
    @param bucket_name: The name of the bucket to use
    @param prefix: The path to list under
    @return: A generator of all the files in the requested path
    """
    for obj in list_s3_objects(s3_client, bucket_name, prefix):
        if obj["Key"][-1] != "/":
            yield obj


def read_dataframe_from_s3(s3_client: boto3.client, bucket_name: str, key: str):
    """
    Read a dataframe from an S3 object
    @param s3_client: The AWS S3 client to use
    @param bucket_name: The name of the bucket to use
    @param key: The key to read from
    @return: The dataframe
    """
    with TemporaryDirectory() as work_dir_name:
        local_file_path = f"{work_dir_name}/{os.path.basename(key)}"
        s3_client.download_file(bucket_name, key, local_file_path)
        return read_dataframe_by_file_path(local_file_path)


def determine_if_bucket_exists(s3_client: boto3.client, bucket_name: str):
    """
    Determine if a bucket exists
    @param s3_client: The AWS S3 client to use
    @param bucket_name: The name of the bucket to use
    @return: True if the bucket exists, False otherwise
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False
