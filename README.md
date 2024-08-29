# Iceberg Demo with AWS Glue and Athena

This project demonstrates the use of Apache Iceberg table format with AWS Glue and Athena. It processes CSV files, stores the data in Iceberg format, and allows querying via Athena.

## Prerequisites

- Python 3.7+
- AWS account with appropriate permissions
- AWS CLI installed

## AWS Credentials Setup

This project uses environment variables for AWS credentials. You have two options to set these up:

### Option 1: Using a .env file (Recommended for local development)

1. Copy the `.env.example` file to a new file named `.env`:
   ```
   cp .env.example .env
   ```

2. Edit the `.env` file and fill in your AWS credentials:
   ```
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   AWS_DEFAULT_REGION=your_preferred_region
   ```

   Replace `your_access_key`, `your_secret_key`, and `your_preferred_region` with your actual AWS credentials and preferred region.

### Option 2: Exporting environment variables

Alternatively, you can set your AWS credentials as environment variables. In your terminal, run:
`export AWS_ACCESS_KEY_ID=your_access_key`
`export AWS_SECRET_ACCESS_KEY=your_secret_key`
`export AWS_DEFAULT_REGION=your_preferred_region`

Replace `your_access_key`, `your_secret_key`, and `your_preferred_region` with your actual AWS credentials and preferred region.

**Note:** If you're using environment variables, make sure they are set in the same terminal session where you're running the scripts.

Choose the method that works best for your environment and security requirements. The .env file method is generally preferred for development as it persists across sessions and is less likely to expose credentials accidentally.


Choose the method that works best for your environment and security requirements.

## Setup

1. Clone this repository:
   ```
   git clone https://github.com/your-username/iceberg-demo.git
   cd iceberg-demo
   ```

2. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

3. Update the `config/config.yaml` file with your specific settings:
   - Ensure the `aws.region` is set to your desired AWS region
   - Update the `input.sample_data_urls` with the URLs of your sample CSV files

## Usage

The demo is split into two main steps: setup and job execution.

### Step 1: Setup

Run the setup script to create the necessary AWS resources:
`python setup.py`

This script will:
- Create an S3 bucket
- Set up an Athena workgroup
- Create a Glue database and tables
- Create an IAM role for Glue
- Create a Glue job

After successful execution, the script will save the created resource information to `resources.yaml`.

### Step 2: Run the Job

To process a CSV file, use the `run_job.py` script:
`python run_job.py <csv_url>`
Replace `<csv_url>` with the URL of the CSV file you want to process.

For example:
python run_job.py https://raw.githubusercontent.com/your-username/your-repo/main/data/sample1.csv
python run_job.py https://raw.githubusercontent.com/your-username/your-repo/main/data/sample2.csv

This script will:
1. Download the CSV file
2. Upload it to the S3 bucket
3. Run the Glue job to process the data
4. Export the results to a CSV file in the S3 bucket

## Results

After running the job, you can find the results in the S3 bucket created during setup. The results will be in the `results/` folder, named `result-1.csv` and `result-2.csv` for the first and second runs respectively.

## Cleaning Up

To avoid unnecessary AWS charges, remember to delete the resources created by this demo when you're done. This includes:
- S3 bucket
- Athena workgroup
- Glue database and job
- IAM role

You can delete these resources manually through the AWS Management Console.

## Troubleshooting

If you encounter any issues:
1. Ensure your AWS credentials are correctly set up and have the necessary permissions.
2. Check the AWS CloudWatch logs for the Glue job for any error messages.
3. Verify that the CSV files are accessible and in the correct format.
4. If you're using environment variables for credentials, make sure they are set in the same terminal session where you're running the scripts.
