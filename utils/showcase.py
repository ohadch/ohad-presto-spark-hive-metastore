import os
from logging import getLogger

from utils.aws import upload_file_to_bucket, create_client, determine_if_bucket_exists
from utils.hive import create_hive_table_by_s3_dir

UPLOADED_FILE_PATH = os.path.join("data", "test_parquet.parquet")

logger = getLogger(__name__)


def upload_showcase_file(bucket_name: str, bucket_dir: str, local_file_path: str):
    """
    Upload the showcase file to S3.
    :param bucket_name: The name of the S3 bucket to use.
    :param bucket_dir: The path to the S3 directory to use.
    :param local_file_path: The path to the local file to upload.
    :return: The bucket name and key of the uploaded file.
    """
    s3_client = create_client("s3")

    # Create the bucket if it doesn't exist
    if not determine_if_bucket_exists(s3_client, bucket_name):
        logger.info(f"Creating S3 bucket {bucket_name}")
        s3_client.create_bucket(Bucket=bucket_name)

    # Upload the file to S3
    s3_target_key = f"{bucket_dir}/{os.path.basename(local_file_path)}"
    upload_file_to_bucket(
        bucket_name=bucket_name,
        file_path=local_file_path,
        key=s3_target_key,
    )


def create_showcase_table(db_name: str, table_name: str, bucket_name: str, bucket_dir: str):
    """
    Creates the showcase table in Hive.
    :param db_name: The name of the Hive database to create the table in.
    :param table_name: The name of the Hive table to create.
    :param bucket_name: The name of the S3 bucket to use.
    :param bucket_dir: The path to the S3 directory to use.
    :return: The name of the Hive table created.
    """
    logger.info(f"Creating showcase table {table_name} in database {db_name}")

    return create_hive_table_by_s3_dir(
        db_name=db_name,
        table_name=table_name,
        s3_bucket=bucket_name,
        s3_dir=bucket_dir,
        partition_keys=[]
    )
