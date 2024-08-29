import json
from botocore.exceptions import ClientError

def create_glue_service_role(iam_client, role_name, bucket_name):
    try:
        trust_relationship = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_relationship)
        )
        
        # Attach AWSGlueServiceRole managed policy
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        )
        
        # Your specific S3 policy
        s3_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:PutObjectAcl",
                        "s3:DeleteObject",
                        "s3:GetObject",  # Added for reading data
                        "s3:ListBucket"  # Added for listing bucket contents
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                }
            ]
        }
        
        # Additional minimal Glue permissions
        glue_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:*",
                        "s3:GetBucketLocation",
                        "s3:ListAllMyBuckets",
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
        # Combine the policies
        combined_policy = {
            "Version": "2012-10-17",
            "Statement": s3_policy_document["Statement"] + glue_policy_document["Statement"]
        }
        
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=f"{role_name}-policy",
            PolicyDocument=json.dumps(combined_policy)
        )
        
        print(f"IAM role '{role_name}' created successfully.")
        return iam_client.get_role(RoleName=role_name)['Role']['Arn']
    except ClientError as e:
        print(f"Error creating IAM role: {e}")
        return None