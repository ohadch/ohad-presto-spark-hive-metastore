from hive_metastore_client import HiveMetastoreClient
from hive_metastore_client.builders import DatabaseBuilder
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import Table

from settings import HIVE_HOST, HIVE_PORT


def create_hive_database(name: str):
    print(f"Creating Hive database {name}")
    database = DatabaseBuilder(name=name).build()
    with HiveMetastoreClient(HIVE_HOST, HIVE_PORT) as hive_metastore_client:
        hive_metastore_client.create_database(database)


def create_hive_table(table: Table):
    print(f"Creating Hive table {table.dbName}.{table.tableName}")
    with HiveMetastoreClient(HIVE_HOST, HIVE_PORT) as hive_metastore_client:
        hive_metastore_client.create_external_table(table)
