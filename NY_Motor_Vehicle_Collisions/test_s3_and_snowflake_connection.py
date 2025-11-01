import os
import psycopg2
import snowflake.connector
from dotenv import load_dotenv

#Load your existing .env file
load_dotenv()

#PGSQL connection test
try:
    pg_conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM nyc_motor_vehicle_collisions;")
        count = cur.fetchone()[0]
        print(f"PostgreSQL connection OK - {count} rows in 'nyc_motor_vehicle_collisions'")
    pg_conn.close()
except Exception as e:
    print("PostgreSQL connection failed:", e)

#Snowflake connection test
try:
    sf_conn = snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database=os.getenv("SF_DATABASE"),
        schema=os.getenv("SF_SCHEMA"),
    )
    cur = sf_conn.cursor()
    cur.execute("SELECT CURRENT_VERSION();")
    print(f"Snowflake connection OK - version {cur.fetchone()[0]}")
    sf_conn.close()
except Exception as e:
    print("Snowflake connection failed:", e)
