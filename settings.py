import os

AWS_REGION = os.environ["AWS_REGION"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

BUCKET_NAME = os.environ["BUCKET_NAME"]
BUCKET_DIR = os.environ["BUCKET_DIR"]

HIVE_HOST = os.environ["HIVE_HOST"]
HIVE_PORT = os.environ["HIVE_PORT"]

LOCALSTACK_HOST = os.environ.get("LOCALSTACK_HOST", "localhost")
LOCALSTACK_URL = f"http://{LOCALSTACK_HOST}:4566"
