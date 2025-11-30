import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import pandas as pd
import geopandas as gpd

NEIGHBORHOOD_COLUMN = "ntaname"
CHUNK_SIZE = 50000

def get_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing environment variable: {name}")
    return value

def load_neighborhoods(geojson_path):
    gdf = gpd.read_file(geojson_path)
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    else:
        gdf = gdf.to_crs(epsg=4326)
    if NEIGHBORHOOD_COLUMN not in gdf.columns:
        raise RuntimeError(f"Neighborhood column '{NEIGHBORHOOD_COLUMN}' not found in GeoJSON")
    return gdf[[NEIGHBORHOOD_COLUMN, "geometry"]]

def connect_snowflake():
    # Prompt user for the current 6-digit MFA code from the authenticator app
    passcode = input("Enter current Snowflake MFA code: ").strip()

    conn = snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        passcode=passcode,
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database=os.getenv("SF_DATABASE"),
        schema=os.getenv("SF_SCHEMA"),
        role=os.getenv("SF_ROLE"),
    )
    return conn


def truncate_target(conn, database, schema, table):
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {database}.{schema}.{table}")
    finally:
        cur.close()

def spatial_join_chunk(df, neighborhoods):
    if "LATITUDE" not in df.columns or "LONGITUDE" not in df.columns:
        raise RuntimeError("Source table must contain LATITUDE and LONGITUDE columns")
    df["NEIGHBORHOOD"] = None
    mask = df["LATITUDE"].notna() & df["LONGITUDE"].notna()
    if not mask.any():
        return df
    df_points = df.loc[mask, ["LATITUDE", "LONGITUDE"]].copy()
    gdf_points = gpd.GeoDataFrame(
        df_points,
        geometry=gpd.points_from_xy(df_points["LONGITUDE"], df_points["LATITUDE"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(gdf_points, neighborhoods, how="left", predicate="within")
    neighborhoods_series = joined[NEIGHBORHOOD_COLUMN]
    df.loc[mask, "NEIGHBORHOOD"] = neighborhoods_series.reindex(df_points.index)
    return df

def process():
    load_dotenv()
    sf_database = get_env("SF_DATABASE")
    sf_source_schema = get_env("SF_SOURCE_SCHEMA")
    sf_target_schema = get_env("SF_SCHEMA")
    sf_table = get_env("SF_TABLE")
    geojson_path = get_env("NEIGHBORHOOD_GEOJSON_PATH")

    neighborhoods = load_neighborhoods(geojson_path)
    conn = connect_snowflake()

    source_table = f"{sf_database}.{sf_source_schema}.{sf_table}"
    target_table = f"{sf_database}.{sf_target_schema}.{sf_table}"

    truncate_target(conn, sf_database, sf_target_schema, sf_table)

    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {source_table}")
        col_names = [c[0] for c in cur.description]
        while True:
            rows = cur.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            df = pd.DataFrame(rows, columns=col_names)
            df = spatial_join_chunk(df, neighborhoods)
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name=sf_table,
                schema=sf_target_schema,
                database=sf_database,
                quote_identifiers=False,
                auto_create_table=False,
            )
            if not success:
                raise RuntimeError("write_pandas reported failure")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    process()
