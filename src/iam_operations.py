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
        
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                        "s3:GetObject",
                        "s3:PutObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:*",
                        "athena:*"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=f"{role_name}-policy",
            PolicyDocument=json.dumps(policy_document)
        )
        
        print(f"IAM role '{role_name}' created successfully.")
        return iam_client.get_role(RoleName=role_name)['Role']['Arn']
    except ClientError as e:
        print(f"Error creating IAM role: {e}")
        return None