import os
import geopandas as gpd
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

CHUNK_SIZE = 50000
NEIGHBORHOOD_COLUMN = "ntaname"
COLLISION_ID_COL = "COLLISION_ID"


def get_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def load_neighborhoods(geojson_path):
    gdf = gpd.read_file(geojson_path)
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    else:
        gdf = gdf.to_crs(epsg=4326)

    if NEIGHBORHOOD_COLUMN not in gdf.columns:
        raise RuntimeError(
            f"Neighborhood column '{NEIGHBORHOOD_COLUMN}' not found in GeoJSON"
        )

    return gdf[[NEIGHBORHOOD_COLUMN, "geometry"]]


def connect_snowflake():
    passcode = input("Enter current Snowflake MFA code: ").strip()
    conn = snowflake.connector.connect(
        user=get_env("SF_USER"),
        password=get_env("SF_PASSWORD"),
        passcode=passcode,
        account=get_env("SF_ACCOUNT"),
        warehouse=get_env("SF_WAREHOUSE"),
        database=get_env("SF_DATABASE"),
        schema=get_env("SF_SCHEMA"),
        role=get_env("SF_ROLE"),
    )
    return conn


def spatial_join_chunk(df, neighborhoods):
    """
    Given a chunk with COLLISION_ID, LATITUDE, LONGITUDE,
    return a DataFrame with COLLISION_ID and NEIGHBORHOOD.
    """
    if "LATITUDE" not in df.columns or "LONGITUDE" not in df.columns:
        raise RuntimeError("Chunk is missing LATITUDE / LONGITUDE")

    df = df[[COLLISION_ID_COL, "LATITUDE", "LONGITUDE"]].copy()
    df["NEIGHBORHOOD"] = None

    mask = df["LATITUDE"].notna() & df["LONGITUDE"].notna()
    if not mask.any():
        return df[[COLLISION_ID_COL, "NEIGHBORHOOD"]]

    df_points = df.loc[mask, ["LATITUDE", "LONGITUDE"]].copy()

    gdf_points = gpd.GeoDataFrame(
        df_points,
        geometry=gpd.points_from_xy(df_points["LONGITUDE"], df_points["LATITUDE"]),
        crs="EPSG:4326",
    )

    joined = gpd.sjoin(gdf_points, neighborhoods, how="left", predicate="within")
    neighborhoods_series = joined[NEIGHBORHOOD_COLUMN]

    df.loc[mask, "NEIGHBORHOOD"] = neighborhoods_series.reindex(df_points.index)

    return df[[COLLISION_ID_COL, "NEIGHBORHOOD"]]


def main():
    load_dotenv()

    sf_database = get_env("SF_DATABASE")
    sf_schema = get_env("SF_SCHEMA")
    sf_table = get_env("SF_TABLE")
    geojson_path = get_env("NEIGHBORHOOD_GEOJSON_PATH")

    neighborhoods = load_neighborhoods(geojson_path)
    conn = connect_snowflake()

    staging_table = "NEIGHBORHOOD_STAGING"

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE OR REPLACE TABLE {sf_database}.{sf_schema}.{staging_table} AS
                SELECT {COLLISION_ID_COL}, CAST(NULL AS VARCHAR) AS NEIGHBORHOOD
                FROM {sf_database}.{sf_schema}.{sf_table}
                WHERE 1 = 0
            """)
            cur.execute(f"TRUNCATE TABLE {sf_database}.{sf_schema}.{staging_table}")

            cur.execute(f"""
                SELECT {COLLISION_ID_COL}, LATITUDE, LONGITUDE
                FROM {sf_database}.{sf_schema}.{sf_table}
                WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
            """)

            total_rows = 0
            while True:
                rows = cur.fetchmany(CHUNK_SIZE)
                if not rows:
                    break

                df_chunk = pd.DataFrame(rows, columns=[COLLISION_ID_COL, "LATITUDE", "LONGITUDE"])
                df_neigh = spatial_join_chunk(df_chunk, neighborhoods)

                success, nchunks, nrows, _ = write_pandas(
                    conn,
                    df_neigh,
                    table_name=staging_table,
                    schema=sf_schema,
                    database=sf_database,
                    auto_create_table=False,
                    quote_identifiers=True,
                )
                if not success:
                    raise RuntimeError("write_pandas reported failure for a chunk")

                total_rows += nrows

            print(f"Wrote {total_rows} rows to {staging_table}")

            cur.execute(f"""
                UPDATE {sf_database}.{sf_schema}.{sf_table} AS T
                SET NEIGHBORHOOD = S.NEIGHBORHOOD
                FROM {sf_database}.{sf_schema}.{staging_table} AS S
                WHERE T.{COLLISION_ID_COL} = S.{COLLISION_ID_COL}
            """)
            print("NEIGHBORHOOD column updated from staging table.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()