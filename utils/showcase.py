import os

from hive_metastore_client.builders import ColumnBuilder, SerDeInfoBuilder, StorageDescriptorBuilder, TableBuilder
from pyspark.sql import SparkSession

from local_settings import BUCKET_NAME, SHOW_CASE_BUCKET_DIR_PATH
from utils.common import create_s3_client
from utils.hive import create_hive_table

UPLOADED_FILE_PATH = os.path.join("data", "alltypes_dictionary.parquet")


def upload_showcase_file():
    s3_client = create_s3_client()
    s3_target_key = f"{SHOW_CASE_BUCKET_DIR_PATH}{os.path.basename(UPLOADED_FILE_PATH)}"

    print(f"Uploading file {UPLOADED_FILE_PATH} to {s3_target_key}")
    s3_client.upload_file(
        Bucket=BUCKET_NAME,
        Key=s3_target_key,
        Filename=UPLOADED_FILE_PATH,
    )
    print("File uploaded successfully.")


def create_showcase_table(db_name: str, table_name: str):
    location = f"s3a://{BUCKET_NAME}/{SHOW_CASE_BUCKET_DIR_PATH}"

    print(f"Creating table {table_name} in database {db_name} with location {location}")

    hive_metastore_columns = [
        ColumnBuilder("id", "string", "col comment").build(),
        ColumnBuilder("name", "string").build(),
    ]

    # If you table has partitions create a list with the partition columns
    # This list is similar to the columns list, and the year, month and day
    # columns are the same.
    partition_keys = [
        #     ColumnBuilder("year", "string").build(),
        #     ColumnBuilder("month", "string").build(),
        #     ColumnBuilder("day", "string").build(),
    ]

    serde_info = SerDeInfoBuilder(
        serialization_lib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    ).build()

    storage_descriptor = StorageDescriptorBuilder(
        columns=hive_metastore_columns,
        location=location,
        input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        serde_info=serde_info,
    ).build()

    table = TableBuilder(
        table_name=table_name,
        db_name=db_name,
        owner="owner name",
        storage_descriptor=storage_descriptor,
        partition_keys=partition_keys,
    ).build()

    create_hive_table(table)
