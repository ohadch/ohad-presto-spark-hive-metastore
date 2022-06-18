import boto3

from settings import AWS_REGION, LOCALSTACK_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def create_client(service_name: str) -> boto3.client:
    """
    Create an S3 client for the tenant.
    :param service_name: Name of the service to create a client for.
    :return: The S3 client.
    """
    return boto3.client(
        service_name,
        region_name=AWS_REGION,
        endpoint_url=LOCALSTACK_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
