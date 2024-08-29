import yaml
import uuid
from src import aws_utils, s3_operations, athena_operations, glue_operations, iam_operations

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def setup_infrastructure(config):
    # Generate unique identifier
    unique_id = str(uuid.uuid4())[:8]
    
    # Initialize AWS clients
    s3_client = aws_utils.create_aws_client('s3')
    athena_client = aws_utils.create_aws_client('athena')
    glue_client = aws_utils.create_aws_client('glue')
    iam_client = aws_utils.create_aws_client('iam')
    
    # Create S3 bucket
    bucket_name = f"{config['s3']['bucket_prefix']}-{unique_id}"
    if not s3_operations.create_s3_bucket(s3_client, bucket_name, config['aws']['region']):
        return
    
    # Create Athena workgroup
    if not athena_operations.create_athena_workgroup(athena_client, config['athena']['workgroup_name'], bucket_name):
        return
    
    # Create Glue database and tables
    if not athena_operations.create_glue_resources(athena_client, config['glue']['database_name'], bucket_name, config['athena']['workgroup_name']):
        return
    
    # Create IAM role for Glue
    role_name = f"AWSGlueServiceRole-IcebergDemo-{unique_id}"
    role_arn = iam_operations.create_glue_service_role(iam_client, role_name, bucket_name)
    if not role_arn:
        return
    
    # Create and upload Glue script
    glue_script = glue_operations.get_glue_script()
    s3_operations.upload_to_s3(s3_client, bucket_name, 'glue-scripts/iceberg_etl.py', glue_script)
    
    # Create Glue job
    script_location = f"s3://{bucket_name}/glue-scripts/iceberg_etl.py"
    if not glue_operations.create_glue_job(glue_client, config['glue']['job_name'], script_location, role_arn, bucket_name):
        return
    
    # Save the created resources info for later use
    resources = {
        'bucket_name': bucket_name,
        'role_arn': role_arn
    }
    with open('resources.yaml', 'w') as file:
        yaml.dump(resources, file)
    
    print("\nSetup completed successfully!")
    print(f"S3 Bucket: {bucket_name}")
    print(f"Athena Workgroup: {config['athena']['workgroup_name']}")
    print(f"Glue Database: {config['glue']['database_name']}")
    print(f"Glue Job: {config['glue']['job_name']}")
    print("\nResource information saved to resources.yaml")
    print("\nNext step: Run the job using run_job.py")

if __name__ == "__main__":
    config = load_config()
    setup_infrastructure(config)