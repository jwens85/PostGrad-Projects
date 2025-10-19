import os
import io
import pandas as pd
import boto3
from sqlalchemy import create_engine
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

#1.Load only PostgreSQL variables (do not override AWS creds)
load_dotenv()

pg_user = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASSWORD")
pg_host = os.getenv("PG_HOST", "localhost")
pg_port = os.getenv("PG_PORT", "5432")
pg_db = os.getenv("PG_DB", "ny_motor_vehicle_collisions")

bucket_name = "jwens85-ny-motor-vehicle-collisions"
key = "NY_Motor_Vehicle_Collisions/Data/NY_Motor_Vehicle_Collisions.csv"
region_name = "us-east-2"

#2.Create a boto3 session bound to your default AWS CLI profile
print("Connecting to S3...")

session = boto3.Session(profile_name="default")
s3 = session.client(
    "s3",
    region_name=region_name,
    config=Config(s3={"addressing_style": "path"})
)

try:
    print(f"Listing objects in bucket: {bucket_name}")
    resp = s3.list_objects_v2(Bucket=bucket_name)
    for o in resp.get("Contents", []):
        print(o["Key"])

    print(f"\nAttempting to get: {key}")
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    csv_buffer = io.BytesIO(obj["Body"].read())
except ClientError as e:
    print(f"Error accessing S3: {e}")
    raise SystemExit(1)

#3.Read CSV into pandas DataFrame
print("Reading CSV into pandas DataFrame...")
df = pd.read_csv(csv_buffer)

#4.Normalize date/time columns
print("Normalizing CRASH DATE and CRASH TIME columns...")
df["CRASH DATE"] = pd.to_datetime(df["CRASH DATE"], format="%m/%d/%Y", errors="coerce").dt.date
df["CRASH TIME"] = pd.to_datetime(df["CRASH TIME"], format="%H:%M", errors="coerce").dt.time
df = df.dropna(subset=["CRASH DATE"])
print(f"DataFrame shape after cleaning: {df.shape}")

#5.Connect to PostgreSQL
print("Connecting to PostgreSQL...")
engine = create_engine(
    f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
)

#6.Insert data into PostgreSQL
print("Inserting data into nyc_motor_vehicle_collisions...")
df.to_sql(
    "nyc_motor_vehicle_collisions",
    engine,
    if_exists="replace",
    index=False,
    chunksize=5000
)

print("Data successfully loaded into PostgreSQL.")