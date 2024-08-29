import boto3
import os
import time

# Use a unique prefix for all resources created by this project
PROJECT_PREFIX = 'iceberg'  # Changed from 'iceberg-demo-'

def delete_s3_bucket(s3_client):
    print("Deleting S3 buckets...")
    deleted_buckets = []
    try:
        response = s3_client.list_buckets()
        for bucket in response['Buckets']:
            if bucket['Name'].startswith(PROJECT_PREFIX):
                print(f"Deleting bucket: {bucket['Name']}")
                bucket_to_delete = boto3.resource('s3').Bucket(bucket['Name'])
                bucket_to_delete.objects.all().delete()
                bucket_to_delete.delete()
                deleted_buckets.append(bucket['Name'])
        print(f"S3 buckets deleted: {deleted_buckets}")
    except Exception as e:
        print(f"Error deleting S3 buckets: {e}")
    return deleted_buckets

def delete_athena_workgroup(athena_client):
    print("Deleting Athena workgroups...")
    deleted_workgroups = []
    try:
        response = athena_client.list_work_groups()
        for workgroup in response['WorkGroups']:
            if workgroup['Name'].startswith(PROJECT_PREFIX):
                print(f"Attempting to delete workgroup: {workgroup['Name']}")
                
                # Delete all named queries in the workgroup
                named_queries = athena_client.list_named_queries(WorkGroup=workgroup['Name'])
                for query_id in named_queries.get('NamedQueryIds', []):
                    athena_client.delete_named_query(NamedQueryId=query_id)
                
                # Attempt to delete the workgroup
                try:
                    athena_client.delete_work_group(WorkGroup=workgroup['Name'])
                    print(f"Workgroup {workgroup['Name']} deleted successfully.")
                    deleted_workgroups.append(workgroup['Name'])
                except athena_client.exceptions.InvalidRequestException as e:
                    if "WorkGroup is not empty" in str(e):
                        print(f"Cannot delete non-empty workgroup {workgroup['Name']}. Manual deletion required.")
                    else:
                        raise
        
        print(f"Athena workgroups deleted: {deleted_workgroups}")
    except Exception as e:
        print(f"Error during Athena workgroup deletion process: {e}")
    return deleted_workgroups

def delete_glue_resources(glue_client):
    print("Deleting Glue resources...")
    deleted_databases = []
    deleted_tables = []
    deleted_jobs = []
    try:
        # Delete Glue databases and tables
        databases = glue_client.get_databases()['DatabaseList']
        for db in databases:
            if db['Name'].lower().startswith(PROJECT_PREFIX.lower()):
                print(f"Deleting Glue database: {db['Name']}")
                try:
                    # First, delete all tables in the database
                    tables = glue_client.get_tables(DatabaseName=db['Name'])['TableList']
                    for table in tables:
                        glue_client.delete_table(DatabaseName=db['Name'], Name=table['Name'])
                        deleted_tables.append(f"{db['Name']}.{table['Name']}")
                        print(f"  Deleted table: {table['Name']}")
                    
                    # Then delete the database
                    glue_client.delete_database(Name=db['Name'])
                    deleted_databases.append(db['Name'])
                    print(f"Glue database {db['Name']} deleted successfully.")
                except Exception as e:
                    print(f"Error deleting Glue database {db['Name']}: {e}")

        # Delete Glue jobs
        jobs = glue_client.get_jobs()['Jobs']
        for job in jobs:
            if job['Name'].lower().startswith(PROJECT_PREFIX.lower()):
                print(f"Deleting Glue job: {job['Name']}")
                try:
                    glue_client.delete_job(JobName=job['Name'])
                    deleted_jobs.append(job['Name'])
                    print(f"Glue job {job['Name']} deleted successfully.")
                except Exception as e:
                    print(f"Error deleting Glue job {job['Name']}: {e}")
        
        print(f"Glue databases deleted: {deleted_databases}")
        print(f"Glue tables deleted: {deleted_tables}")
        print(f"Glue jobs deleted: {deleted_jobs}")
    except Exception as e:
        print(f"Error during Glue resources deletion process: {e}")
    return deleted_databases, deleted_tables, deleted_jobs

def delete_iam_role(iam_client):
    print("Deleting IAM roles...")
    deleted_roles = []
    try:
        paginator = iam_client.get_paginator('list_roles')
        for page in paginator.paginate():
            for role in page['Roles']:
                if role['RoleName'].startswith('AWSGlueServiceRole') and 'IcebergDemo' in role['RoleName']:
                    print(f"Deleting IAM role: {role['RoleName']}")
                    try:
                        # Detach managed policies
                        attached_policies = iam_client.list_attached_role_policies(RoleName=role['RoleName'])['AttachedPolicies']
                        for policy in attached_policies:
                            iam_client.detach_role_policy(RoleName=role['RoleName'], PolicyArn=policy['PolicyArn'])
                            print(f"  Detached policy: {policy['PolicyName']}")
                        
                        # Delete inline policies
                        inline_policies = iam_client.list_role_policies(RoleName=role['RoleName'])['PolicyNames']
                        for policy_name in inline_policies:
                            iam_client.delete_role_policy(RoleName=role['RoleName'], PolicyName=policy_name)
                            print(f"  Deleted inline policy: {policy_name}")
                        
                        # Delete role
                        iam_client.delete_role(RoleName=role['RoleName'])
                        deleted_roles.append(role['RoleName'])
                        print(f"IAM role {role['RoleName']} deleted successfully.")
                    except iam_client.exceptions.DeleteConflictException:
                        print(f"  Cannot delete role {role['RoleName']} due to a delete conflict. It may be an AWS service-linked role.")
                    except Exception as e:
                        print(f"  Error deleting IAM role {role['RoleName']}: {e}")
        print(f"IAM roles deleted: {deleted_roles}")
    except Exception as e:
        print(f"Error during IAM role deletion process: {e}")
    return deleted_roles

def main():
    s3_client = boto3.client('s3')
    athena_client = boto3.client('athena')
    glue_client = boto3.client('glue')
    iam_client = boto3.client('iam')

    deleted_buckets = delete_s3_bucket(s3_client)
    time.sleep(2)  # 2-second delay

    deleted_workgroups = delete_athena_workgroup(athena_client)
    time.sleep(2)  # 2-second delay

    deleted_databases, deleted_tables, deleted_jobs = delete_glue_resources(glue_client)
    time.sleep(2)  # 2-second delay

    deleted_roles = delete_iam_role(iam_client)

    if os.path.exists('resources.yaml'):
        os.remove('resources.yaml')
        print("resources.yaml file removed.")

    print("\nSummary of deleted resources:")
    print(f"S3 Buckets: {deleted_buckets}")
    print(f"Athena Workgroups: {deleted_workgroups}")
    print(f"Glue Databases: {deleted_databases}")
    print(f"Glue Tables: {deleted_tables}")
    print(f"Glue Jobs: {deleted_jobs}")
    print(f"IAM Roles: {deleted_roles}")

    if not any([deleted_buckets, deleted_workgroups, deleted_databases, deleted_tables, deleted_jobs, deleted_roles]):
        print("\nNo resources matching the project prefix were found or deleted.")
    else:
        print("\nCleanup process completed. Please review the summary above.")

    print("\nIf any resources couldn't be deleted automatically, manual cleanup may be required.")
    print("You can run this script again to attempt cleanup of any remaining resources.")

if __name__ == "__main__":
    main()