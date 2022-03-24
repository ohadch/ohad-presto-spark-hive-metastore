import boto3
from botocore.config import Config

from local_settings import AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def create_client(service_name, *args, **kwargs):
    return boto3.client(
        service_name,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        *args,
        **kwargs,
    )


def create_s3_client(config=Config(signature_version="s3v4"), *args, **kwargs):
    return create_client(service_name="s3", config=config, *args, **kwargs)
