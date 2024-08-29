from botocore.exceptions import ClientError
import time

def create_athena_workgroup(athena_client, workgroup_name, bucket_name):
    try:
        athena_client.create_work_group(
            Name=workgroup_name,
            Description='Workgroup for Iceberg table format demo',
            Configuration={
                'ResultConfiguration': {
                    'OutputLocation': f's3://{bucket_name}/athena-results/'
                },
                'EngineVersion': {
                    'SelectedEngineVersion': 'Athena engine version 3'
                }
            }
        )
        print(f"Workgroup '{workgroup_name}' created successfully.")
        return True
    except ClientError as e:
        print(f"Error creating Athena workgroup: {e}")
        return False

def run_athena_query(athena_client, query, database, workgroup):
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            WorkGroup=workgroup
        )
        query_execution_id = response['QueryExecutionId']
        
        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = query_status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return state
            time.sleep(5)
    except ClientError as e:
        print(f"Error running Athena query: {e}")
        return 'FAILED'

def create_glue_resources(athena_client, database_name, bucket_name, workgroup_name):
    queries = [
        f"CREATE DATABASE IF NOT EXISTS {database_name}",
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.csv_input(
        op string,
        product_id bigint,
        category string,
        product_name string,
        quantity_available bigint,
        last_update_time string)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://{bucket_name}/raw-input/'
        TBLPROPERTIES (
        'areColumnsQuoted'='false',
        'classification'='csv',
        'columnsOrdered'='true',
        'compressionType'='none',
        'delimiter'=',',
        'typeOfData'='file')
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {database_name}.iceberg_table (
        product_id bigint,
        category string,
        product_name string,
        quantity_available bigint,
        last_update_time timestamp)
        PARTITIONED BY (category, bucket(16,product_id))
        LOCATION 's3://{bucket_name}/iceberg-output/'
        TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_target_data_file_size_bytes'='536870912'
        )
        """
    ]
    
    for query in queries:
        state = run_athena_query(athena_client, query, database_name, workgroup_name)
        if state != 'SUCCEEDED':
            print(f"Query failed with state: {state}")
            return False
    return True

def export_to_csv(athena_client, query, database, workgroup, output_location):
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location},
            WorkGroup=workgroup
        )
        query_execution_id = response['QueryExecutionId']
        
        while True:
            status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                print(f"Athena query finished with status: {status}")
                break
            print(f"Athena query status: {status}. Waiting...")
            time.sleep(5)
    except ClientError as e:
        print(f"Error exporting to CSV: {e}")