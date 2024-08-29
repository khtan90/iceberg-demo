from botocore.exceptions import ClientError
import time
import os

def get_glue_script():
    script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'iceberg.py')
    with open(script_path, 'r') as file:
        return file.read()

def create_glue_job(glue_client, job_name, script_location, role_arn, bucket_name):
    try:
        glue_client.create_job(
            Name=job_name,
            Role=role_arn,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': script_location,
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--job-language': 'python',
                '--job-bookmark-option': 'job-bookmark-enable',
                '--enable-glue-datacatalog': '',
                '--datalake-formats': 'iceberg',
                '--iceberg_job_catalog_warehouse': f's3://{bucket_name}/iceberg-warehouse/'
            },
            GlueVersion='4.0',
            WorkerType='G.1X',
            NumberOfWorkers=2,
            Timeout=2880
        )
        print(f"Glue job '{job_name}' created successfully.")
        return True
    except ClientError as e:
        print(f"Error creating Glue job: {e}")
        return False

def run_glue_job(glue_client, job_name):
    try:
        response = glue_client.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        print(f"Started Glue job '{job_name}' with run ID: {job_run_id}")
        
        while True:
            status = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
            if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                print(f"Glue job finished with status: {status}")
                break
            print(f"Glue job status: {status}. Waiting...")
            time.sleep(30)
    except ClientError as e:
        print(f"Error running Glue job: {e}")