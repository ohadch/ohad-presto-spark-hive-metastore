from dotenv import load_dotenv
load_dotenv()


def main():
    from utils.hive import upload_files_and_register_in_metastore

    upload_files_and_register_in_metastore(
        bucket_name="spark",
        bucket_base_dir="warehouse/",
        metastore_db_name="ohad_db",
        metastore_table_name="ohad_table",
        local_file_paths=[
            "assets/test_csv.csv",
        ]
    )


if __name__ == '__main__':
    main()
