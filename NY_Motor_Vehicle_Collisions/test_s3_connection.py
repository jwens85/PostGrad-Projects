#Test_s3_connection.py
#Verifies access to your S3 bucket using boto3 and correct virtual-host addressing.

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

#Configuration
region_name = "us-east-2"
bucket_name = "jwens85-ny-motor-vehicle-collisions"

#Force virtual-host addressing (required for correct SignatureV4 signing)
s3_config = Config(
    region_name=region_name,
    s3={"addressing_style": "virtual"}
)

print(f"Using region: {region_name}")
print(f"Testing access to bucket: {bucket_name}")

#Test the connection
try:
    s3 = boto3.client("s3", config=s3_config)
    response = s3.list_objects_v2(Bucket=bucket_name)
    print("Successfully accessed S3 bucket.")
    for obj in response.get("Contents", []):
        print(obj["Key"])
except ClientError as e:
    print(f"\nError: {e}")
