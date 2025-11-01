import os
import snowflake.connector
from dotenv import load_dotenv

# Load Snowflake credentials from .env
load_dotenv(os.path.join("NY_Motor_Vehicle_Collisions", ".env"))

try:
    conn = snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database=os.getenv("SF_DATABASE"),
        schema=os.getenv("SF_SCHEMA")
    )
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_VERSION();")
    version = cur.fetchone()[0]
    print(f"Connected to Snowflake successfully! Version: {version}")
except Exception as e:
    print(f"Connection failed: {e}")
finally:
    try:
        cur.close()
        conn.close()
    except:
        pass
