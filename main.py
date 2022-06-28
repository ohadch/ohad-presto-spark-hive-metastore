from dotenv import load_dotenv
load_dotenv()

from utils.entrypoints import showcase_table_creation_using_hive


def main():
    showcase_table_creation_using_hive("assets/test_parquet.parquet")


if __name__ == '__main__':
    main()
