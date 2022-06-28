import os
from logging import getLogger

from utils.aws import upload_file_to_bucket
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
    return create_hive_table_by_s3_dir(
        db_name=db_name,
        table_name=table_name,
        s3_bucket=bucket_name,
        s3_dir=bucket_dir,
        partition_keys=[]
    )
