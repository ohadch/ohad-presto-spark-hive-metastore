from utils.hive import create_hive_database
from utils.showcase import upload_showcase_file, create_showcase_table, create_spark_session

SHOWCASE_DB_NAME = "showcase_db"
SHOWCASE_TABLE_NAME = "showcase_table"


def main():
    upload_showcase_file()
    create_hive_database(SHOWCASE_DB_NAME)
    create_showcase_table(SHOWCASE_DB_NAME, SHOWCASE_TABLE_NAME)
    # create_spark_session()


if __name__ == '__main__':
    main()
