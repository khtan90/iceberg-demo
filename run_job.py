import yaml
import argparse
from src import aws_utils, s3_operations, athena_operations, glue_operations, data_operations

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def load_resources():
    with open('resources.yaml', 'r') as file:
        return yaml.safe_load(file)

def process_csv(s3_client, glue_client, athena_client, bucket_name, config, url, file_number):
    # Download and upload sample data
    sample_data = data_operations.download_sample_data(url)
    if sample_data:
        s3_operations.upload_to_s3(s3_client, bucket_name, f'raw-input/sample-data-{file_number}.csv', sample_data)
    
    # Run Glue job
    glue_operations.run_glue_job(glue_client, config['glue']['job_name'])
    
    # Export results to CSV
    query = f"SELECT * FROM {config['glue']['database_name']}.iceberg_table"
    result_location = f"s3://{bucket_name}/results/result-{file_number}.csv"
    athena_operations.export_to_csv(athena_client, query, config['glue']['database_name'], config['athena']['workgroup_name'], result_location)

def main(csv_url):
    config = load_config()
    resources = load_resources()
    
    # Initialize AWS clients
    s3_client = aws_utils.create_aws_client('s3')
    athena_client = aws_utils.create_aws_client('athena')
    glue_client = aws_utils.create_aws_client('glue')
    
    # Process CSV
    file_number = '1' if csv_url == config['input']['sample_data_urls'][0] else '2'
    process_csv(s3_client, glue_client, athena_client, resources['bucket_name'], config, csv_url, file_number)
    
    print(f"\nProcessing of CSV {file_number} completed successfully!")
    print(f"Results location: s3://{resources['bucket_name']}/results/result-{file_number}.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process a CSV file through the Iceberg demo pipeline.')
    parser.add_argument('csv_url', help='URL of the CSV file to process')
    args = parser.parse_args()
    
    main(args.csv_url)