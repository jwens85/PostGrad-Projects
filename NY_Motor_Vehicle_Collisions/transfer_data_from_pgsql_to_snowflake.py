import os
import math
import pandas as pd
import numpy as np
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine, text

#Load environment variables from .env file
load_dotenv()

#PGSQL .env credentials
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_TABLE = "nyc_motor_vehicle_collisions"

#Snowflake .env credientials
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
SF_TABLE = os.getenv("SF_TABLE", "NYC_MOTOR_VEHICLE_COLLISIONS")
SF_ROLE = os.getenv("SF_ROLE", "ACCOUNTADMIN")

#Chunk to 200K rows
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "200000"))

def get_pg_engine():
    url = (
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
        f"@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    )
    return create_engine(url, pool_pre_ping=True)

def get_sf_connection():
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE,
    )
    cur = conn.cursor()
    cur.execute("SHOW DATABASES;")
    dbs = [row[1] for row in cur.fetchall()]
    print("Databases visible in this session:", dbs)
    cur.execute(f"USE WAREHOUSE {SF_WAREHOUSE};")
    cur.execute(f"USE DATABASE {SF_DATABASE};")
    cur.execute(f"USE SCHEMA {SF_SCHEMA};")
    cur.execute(
        "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();"
    )
    print("Snowflake session context:", cur.fetchone())
    return conn

def count_pg_rows(pg_engine):
    with pg_engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {PG_TABLE};")).scalar_one()

def _format_seconds_to_hms_int64(sec_series: pd.Series) -> pd.Series:
    s = sec_series.astype("Int64")
    is_na = s.isna()
    sec = s.fillna(0).to_numpy(dtype=np.int64)

    hrs = sec // 3600
    rem = sec % 3600
    mins = rem // 60
    secs = rem % 60

    out = pd.Series([f"{h:02d}:{m:02d}:{s:02d}" for h, m, s in zip(hrs, mins, secs)], index=sec_series.index)
    out[is_na] = None
    return out

def _coerce_time_like_series(ser: pd.Series) -> pd.Series:
    if pd.api.types.is_timedelta64_dtype(ser):
        base = pd.Timestamp("1970-01-01")
        return (base + ser).dt.strftime("%H:%M:%S")

    if pd.api.types.is_integer_dtype(ser) or pd.api.types.is_float_dtype(ser):
        s = pd.to_numeric(ser, errors="coerce")
        if s.notna().sum() == 0:
            return ser

        max_val = s.abs().max()
        if max_val > 1e9:
            secs = (s // 1_000_000_000).astype("Int64")
            return _format_seconds_to_hms_int64(secs)
        else:
            secs = s.round().astype("Int64")
            return _format_seconds_to_hms_int64(secs)

    if pd.api.types.is_object_dtype(ser) or pd.api.types.is_string_dtype(ser):
        parsed = pd.to_datetime(ser, errors="coerce").dt.time
        if parsed.notna().any():
            out = pd.Series(
                [t.strftime("%H:%M:%S") if pd.notna(t) else None for t in parsed],
                index=ser.index,
            )
            return out

        #Clean time formatting
        cleaned = ser.astype(str).str.strip()
        #Keep only digits and colon
        cleaned = cleaned.str.replace(r"[^0-9:]", "", regex=True)

        def normalize_piece(x: str) -> str:
            if ":" in x:
                #Format HH:MM or HH:MM:SS
                parts = x.split(":")
                parts = [p.zfill(2) for p in parts]
                if len(parts) == 2:
                    parts.append("00")
                return f"{parts[0]}:{parts[1]}:{parts[2]}"
            else:
                #Zero-padded times
                if len(x) <= 2:
                    return f"{x.zfill(2)}:00:00"
                elif len(x) == 3:
                    return f"{x[0].zfill(2)}:{x[1:].zfill(2)}:00"
                elif len(x) >= 4:
                    return f"{x[-4:-2].zfill(2)}:{x[-2:].zfill(2)}:00"

        return cleaned.map(normalize_piece)

    #Fallback: leave unchanged
    return ser

def normalize_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    #Uppercase columns
    out.columns = [c.upper() for c in out.columns]

    #Detect date columns by name and coerce to date
    for col in out.columns:
        if "DATE" in col:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.date

    #Detect time columns:
    candidate_cols = set()
    for col in out.columns:
        if "TIME" in col:
            candidate_cols.add(col)
        elif pd.api.types.is_timedelta64_dtype(out[col]):
            candidate_cols.add(col)
        elif pd.api.types.is_integer_dtype(out[col]) or pd.api.types.is_float_dtype(out[col]):
            s = pd.to_numeric(out[col], errors="coerce")
            if s.notna().any() and s.abs().max() > 1e9:
                candidate_cols.add(col)

    for col in candidate_cols:
        out[col] = _coerce_time_like_series(out[col])

    return out


#Main transfer
def transfer_data():
    #Connect to PGSQL using SQLAlchemy
    pg_engine = get_pg_engine()

    total_rows = count_pg_rows(pg_engine)
    num_chunks = math.ceil(total_rows / CHUNK_SIZE)
    print(f"Starting transfer of {total_rows:,} rows in {num_chunks} chunks...")

    #Connect to Snowflake
    sf_conn = get_sf_connection()
    sf_cur = sf_conn.cursor()

    if not SF_DATABASE or not SF_SCHEMA or not SF_TABLE:
        raise RuntimeError("Snowflake identifiers missing (SF_DATABASE/SF_SCHEMA/SF_TABLE). Check your .env.")

    transferred = 0

    #Chunked execution loop
    for i in range(num_chunks):
        offset = i * CHUNK_SIZE
        hi = min(offset + CHUNK_SIZE, total_rows)
        print(f"\nFetching rows {offset:,} to {hi:,} from PostgreSQL...")

        query = text(f"SELECT * FROM {PG_TABLE} OFFSET {offset} LIMIT {CHUNK_SIZE};")
        df = pd.read_sql_query(query, pg_engine)

        if df.empty:
            break

        #Normalize date/time and column names for Snowflake
        df = normalize_for_snowflake(df)

        print(f"Uploading chunk {i+1}/{num_chunks} ({len(df)} rows) to Snowflake...")
        try:
            success, nchunks, nrows, _ = write_pandas(
                conn=sf_conn,
                df=df,
                table_name=SF_TABLE,
                quote_identifiers=True,
            )
        except snowflake.connector.errors.ProgrammingError as e:
            #Print some diagnostics on failure
            print("\nWRITE_PANDAS ERROR — diagnostics:")
            print(f"Error: {e!r}")
            #Show dtypes and a few sample rows of candidate time columns
            print("\nColumn dtypes:")
            print(df.dtypes)
            time_like = [c for c in df.columns if "TIME" in c]
            if time_like:
                print("\nSample time-like columns (head):")
                print(df[time_like].head(10))
            raise

        transferred += nrows
        print(f"Chunk {i+1} complete — {nrows:,} rows loaded (Success={success})")

    #Final verification
    sf_cur.execute(f'SELECT COUNT(*) FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE};')
    sf_count = sf_cur.fetchone()[0]

    print("\nTransfer complete!")
    print(f"Rows transferred: {transferred:,}")
    print(f"Rows in Snowflake: {sf_count:,}")

    sf_conn.close()

if __name__ == "__main__":
    transfer_data()