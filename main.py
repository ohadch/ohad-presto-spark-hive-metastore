from settings import BUCKET_NAME, BUCKET_DIR
from utils.hive import create_hive_database
from utils.showcase import upload_showcase_file, create_showcase_table, create_spark_session

SHOWCASE_DB_NAME = "showcase_db"
SHOWCASE_TABLE_NAME = "showcase_table"


def main():
    bucket_name, bucket_dir = BUCKET_NAME, BUCKET_DIR
    db_name, table_name = SHOWCASE_DB_NAME, SHOWCASE_TABLE_NAME

    upload_showcase_file(
        bucket_name=bucket_name,
        bucket_dir=bucket_dir,
        local_file_path="assets/test_csv.csv"
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


if __name__ == '__main__':
    main()
