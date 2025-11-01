import os
import psycopg2
import snowflake.connector
from dotenv import load_dotenv

#Load .env file
load_dotenv()

#PGSQL .env configs
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_TABLE = "nyc_motor_vehicle_collisions"

#Snowflake .env configs
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
SF_TABLE = "NYC_MOTOR_VEHICLE_COLLISIONS"
SF_ROLE = os.getenv("SF_ROLE", "ACCOUNTADMIN")

#Mapping PGSQL datatypes to Snowflake datatypes
PG_TO_SF = {
    "integer": "NUMBER",
    "bigint": "NUMBER",
    "smallint": "NUMBER",
    "numeric": "NUMBER",
    "double precision": "FLOAT",
    "real": "FLOAT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp without time zone": "TIMESTAMP_NTZ",
    "timestamp with time zone": "TIMESTAMP_TZ",
    "time without time zone": "TIME",
    "character varying": "VARCHAR",
    "character": "VARCHAR",
    "text": "VARCHAR",
    "uuid": "VARCHAR",
}

def fetch_pg_columns():
    #Read column names and datatypes from PGSQL table
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        ORDER BY ordinal_position;
    """, (PG_TABLE,))
    cols = cur.fetchall()
    cur.close()
    conn.close()
    if not cols:
        raise RuntimeError(f"No columns found for table '{PG_TABLE}' in database '{PG_DATABASE}'.")
    print(f"Retrieved {len(cols)} columns from PostgreSQL table '{PG_TABLE}'.")
    return cols

def create_sf_table(columns):
    #Create a table in Snowflake based on the PGSQL schema
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE
    )
    cur = conn.cursor()

    #Diagnostic context info
    cur.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();")
    context = cur.fetchone()
    print(f"Connected to Snowflake context:\n  ROLE={context[0]}, WAREHOUSE={context[1]}, DB={context[2]}, SCHEMA={context[3]}")

    #Validate existence of warehouse, database, schema
    cur.execute(f"SHOW WAREHOUSES LIKE '{SF_WAREHOUSE}';")
    wh_exists = cur.fetchone()
    cur.execute(f"SHOW DATABASES LIKE '{SF_DATABASE}';")
    db_exists = cur.fetchone()

    if not wh_exists:
        raise RuntimeError(f"Warehouse '{SF_WAREHOUSE}' does not exist or is not accessible to your role.")
    if not db_exists:
        raise RuntimeError(f"Database '{SF_DATABASE}' does not exist or is not accessible to your role.")

    #Set current database to avoid NULL context issues
    cur.execute(f'USE DATABASE "{SF_DATABASE}";')
    cur.execute(f"SHOW SCHEMAS LIKE '{SF_SCHEMA}';")
    schema_exists = cur.fetchone()
    if not schema_exists:
        raise RuntimeError(f"Schema '{SF_SCHEMA}' does not exist or is not accessible to your role.")
    cur.execute(f'USE SCHEMA "{SF_SCHEMA}";')

    #Confirm context after USE commands
    cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();")
    db_schema_context = cur.fetchone()
    print(f"Context after USE: DB={db_schema_context[0]}, SCHEMA={db_schema_context[1]}")

    #Create table DDL
    ddl = f'CREATE TABLE IF NOT EXISTS "{SF_DATABASE}"."{SF_SCHEMA}"."{SF_TABLE}" (\n'
    ddl += ",\n".join([f'  "{col.upper()}" {PG_TO_SF.get(dtype, "VARCHAR")}' for col, dtype in columns])
    ddl += "\n);"

    cur.execute(ddl)
    print(f"Created/verified Snowflake table: {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    columns = fetch_pg_columns()
    create_sf_table(columns)