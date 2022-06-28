import os.path

from settings import BUCKET_NAME, METASTORE_DB_NAME
from utils.hive import create_hive_database
from utils.showcase import upload_showcase_file, create_showcase_table


def showcase_table_creation_using_hive(local_file_path: str):
    bucket_name = BUCKET_NAME
    db_name = METASTORE_DB_NAME
    table_name = os.path.basename(local_file_path).split(".")[0]
    bucket_dir = table_name + "/"

    upload_showcase_file(
        bucket_name=bucket_name,
        bucket_dir=bucket_dir,
        local_file_path=local_file_path
    )

    create_hive_database(
        name=db_name
    )

    create_showcase_table(
        db_name=db_name,
        table_name=table_name,
        bucket_name=bucket_name,
        bucket_dir=bucket_dir
    )

    # create_spark_session()