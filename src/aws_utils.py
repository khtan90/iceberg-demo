import boto3
from dotenv import load_dotenv
import os

load_dotenv()

def create_aws_client(service_name):
    return boto3.client(
        service_name,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )