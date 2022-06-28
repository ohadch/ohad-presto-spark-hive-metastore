from typing import List

from hive_metastore_client import HiveMetastoreClient
from hive_metastore_client.builders import DatabaseBuilder, SerDeInfoBuilder, StorageDescriptorBuilder, \
    TableBuilder
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import StorageDescriptor, FieldSchema

from settings import HIVE_HOST, HIVE_PORT
from utils.aws import list_s3_files, create_client, read_dataframe_from_s3
from utils.parsing import get_hive_columns_by_dataframe


def create_hive_database(name: str):
    print(f"Creating Hive database {name}")
    database = DatabaseBuilder(name=name).build()
    with HiveMetastoreClient(HIVE_HOST, HIVE_PORT) as hive_metastore_client:
        hive_metastore_client.create_database(database)


def get_storage_descriptor_by_bucket_location(bucket_name: str, s3_dir: str):
    """
    Get the storage descriptor for a file.
    :param bucket_name: The bucket name.
    :param s3_dir: The S3 directory.
    :return: The storage descriptor.
    """
    # Prepare the required information
    s3_client = create_client(service_name="s3")
    location_files_generator = list_s3_files(
        s3_client=s3_client,
        bucket_name=bucket_name,
        prefix=s3_dir,
    )
    bucket_object = next(location_files_generator)
    remote_file_path = bucket_object["Key"]
    extension = remote_file_path.split(".")[-1]
    bucket_dir_location_uri = f"s3://{bucket_name}/{s3_dir}"

    # Read the dataframe
    df = read_dataframe_from_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        key=remote_file_path,
    )
    hive_metastore_columns = get_hive_columns_by_dataframe(df)

    if extension == "parquet":
        serde_info = SerDeInfoBuilder(
            serialization_lib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        ).build()
        storage_descriptor = StorageDescriptorBuilder(
            location=bucket_dir_location_uri,
            input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            serde_info=serde_info,
            columns=hive_metastore_columns,
        ).build()
    elif extension == "csv":
        serde_info = SerDeInfoBuilder(
            serialization_lib="org.apache.hadoop.hive.serde2.csv.CsvSerDe"
        ).build()
        storage_descriptor = StorageDescriptorBuilder(
            location=bucket_dir_location_uri,
            input_format="org.apache.hadoop.hive.ql.io.CSVInputFormat",
            output_format="org.apache.hadoop.hive.ql.io.CSVOutputFormat",
            serde_info=serde_info,
            columns=hive_metastore_columns,
        ).build()
    else:
        raise ValueError(f"Unsupported file extension {extension}")

    return storage_descriptor


def create_hive_table(db_name: str, table_name: str, storage_descriptor: StorageDescriptor, partition_keys: List[FieldSchema]=list):
    """
    Create a Hive table for a list of files.
    :param db_name: The database name.
    :param table_name: The table name.
    :param storage_descriptor: The storage descriptor.
    :param partition_keys: The partition keys.
        If the table has partitions create a list with the partition columns
        This list is similar to the columns list, and the year, month and day
        columns are the same.
    :return: The Hive table.
    """
    table = TableBuilder(
        table_name=table_name,
        db_name=db_name,
        owner="owner name",
        storage_descriptor=storage_descriptor,
        partition_keys=partition_keys,
    ).build()

    with HiveMetastoreClient(HIVE_HOST, HIVE_PORT) as hive_metastore_client:
        hive_metastore_client.create_external_table(table)


def create_hive_table_by_s3_dir(db_name: str, table_name: str, s3_bucket: str, s3_dir: str, partition_keys: List[FieldSchema]=list):
    """
    Create a Hive table for a S3 directory.
    :param db_name: The database name.
    :param table_name: The table name.
    :param s3_bucket: The S3 bucket name.
    :param s3_dir: The S3 directory.
    :param partition_keys: The partition keys.
        If the table has partitions create a list with the partition columns
        This list is similar to the columns list, and the year, month and day
        columns are the same.
    :return: The Hive table.
    """
    storage_descriptor = get_storage_descriptor_by_bucket_location(
        bucket_name=s3_bucket,
        s3_dir=s3_dir,
    )
    create_hive_table(
        db_name=db_name,
        table_name=table_name,
        storage_descriptor=storage_descriptor,
        partition_keys=partition_keys
    )