from botocore.exceptions import ClientError

def create_s3_bucket(s3_client, bucket_name, region):
    try:
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print(f"Bucket '{bucket_name}' created.")
        return True
    except ClientError as e:
        print(f"Error creating bucket: {e}")
        return False

def upload_to_s3(s3_client, bucket_name, key, body):
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
        print(f"Uploaded to s3://{bucket_name}/{key}")
        return True
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        return False