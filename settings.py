import os
import logging

AWS_REGION = os.environ["AWS_REGION"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

BUCKET_NAME = os.environ["BUCKET_NAME"]

METASTORE_DB_NAME = os.environ["METASTORE_DB_NAME"]

HIVE_HOST = os.environ["HIVE_HOST"]
HIVE_PORT = os.environ["HIVE_PORT"]

LOCALSTACK_HOST = os.environ["LOCALSTACK_HOST"]
LOCALSTACK_PORT = os.environ["LOCALSTACK_PORT"]
LOCALSTACK_URL = f"http://{LOCALSTACK_HOST}:{LOCALSTACK_PORT}"

# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


