from pyspark.sql import SparkSession


def run_spark_demo():
    spark = create_spark_session()
    result = query_spark(spark)
    print(result)


def query_spark(spark: SparkSession):
    return spark.sql("SHOW TABLES").show()


def create_spark_session():
    spark = (
        SparkSession
            .builder
            .master("spark://localhost:7077")
            .appName("SparkHiveMetastoreTest")
            .config("spark.sql.uris", "thrift://localhost:9083")
            .config("hive.metastore.warehouse.dir", "thrift://localhost:9083")
            .enableHiveSupport()
            .getOrCreate()
    )

    return spark
