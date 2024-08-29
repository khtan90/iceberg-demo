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
```
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=your_preferred_region
```


Replace `your_access_key`, `your_secret_key`, and `your_preferred_region` with your actual AWS credentials and preferred region.

**Note:** If you're using environment variables, make sure they are set in the same terminal session where you're running the scripts.

Choose the method that works best for your environment and security requirements. The .env file method is generally preferred for development as it persists across sessions and is less likely to expose credentials accidentally.

## Verifying Your Credentials

After setting up your credentials, you can verify them using these methods:

1. Echo the environment variables:
   ```
   echo $AWS_ACCESS_KEY_ID
   echo $AWS_SECRET_ACCESS_KEY
   echo $AWS_DEFAULT_REGION
   ```

2. Use the AWS CLI to verify your identity:
   ```
   aws sts get-caller-identity
   ```

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
`python3 setup.py`

This script will:
- Create an S3 bucket
- Set up an Athena workgroup
- Create a Glue database and tables
- Create an IAM role for Glue
- Create a Glue job

After successful execution, the script will save the created resource information to `resources.yaml`.

### Step 2: Run the Job

To process a CSV file, use the `run_job.py` script:
- `python3 run_job.py <csv_url>`
- Replace `<csv_url>` with the URL of the CSV file you want to process.

For example:
python run_job.py https://github.com/khtan90/iceberg-demo/blob/main/lab_resources/LOAD00000001.csv
python run_job.py 'https://github.com/khtan90/iceberg-demo/blob/main/lab_resources/incr001.csv'

This script will:
1. Download the CSV file
2. Upload it to the S3 bucket
3. Run the Glue job to process the data
4. Export the results to a CSV file in the S3 bucket

## Results

After running the job, you can find the results in the S3 bucket created during setup. The results will be in the `results/` folder, named `result-1.csv` and `result-2.csv` for the first and second runs respectively.

## Cleaning Up

To avoid unnecessary AWS charges, remember to delete the resources created by this demo when you're done. You can use the provided cleanup script to automatically remove all created resources:

1. Ensure you're in the project root directory.
2. Run the cleanup script:
   ```
   python3 cleanup.py
   ```

This script will delete the following resources:
- S3 bucket
- Athena workgroup
- Glue database and job
- IAM role

It will also remove the `resources.yaml` file.

**Note:** Make sure you run this script with the same AWS credentials used to create the resources. Also, ensure that you no longer need any data stored in these resources before running the cleanup.

If you prefer to delete resources manually, you can do so through the AWS Management Console for each service (S3, Athena, Glue, and IAM).

## Troubleshooting

If you encounter any issues:
1. Ensure your AWS credentials are correctly set up and have the necessary permissions.
2. Check the AWS CloudWatch logs for the Glue job for any error messages.
3. Verify that the CSV files are accessible and in the correct format.
4. If you're using environment variables for credentials, make sure they are set in the same terminal session where you're running the scripts.
