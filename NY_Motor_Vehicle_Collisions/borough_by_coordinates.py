import os
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import exists

#Configurable constants
PRIMARY_KEY_COLUMN = "COLLISION_ID"
TARGET_TABLE_DEFAULT = "NYC_MOTOR_VEHICLE_COLLISIONS"
UPDATE_FLAG_COLUMN = "BOROUGH_UPDATED_MANUALLY"  #BOOLEAN flag for rows we update

def get_env(name: str, default: str | None = None, required: bool = False) -> str:
    """Fetch environment variable or raise an error if required and missing."""
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def load_environment():
    """Load .env file from the current directory."""
    load_dotenv()

def main():
    load_environment()

    #Snowflake .env credentials
    account    = get_env("SF_ACCOUNT",   required=True)
    user       = get_env("SF_USER",      required=True)
    password   = get_env("SF_PASSWORD",  required=True)
    role       = get_env("SF_ROLE",      required=True)
    warehouse  = get_env("SF_WAREHOUSE", required=True)
    database   = get_env("SF_DATABASE",  required=True)
    schema     = get_env("SF_SCHEMA",    required=True)
    table_name = get_env("SF_TABLE", TARGET_TABLE_DEFAULT)

    #Local GeoJSON/Shapefile .env path
    borough_geojson_path = Path(get_env("BOROUGH_GEOJSON_PATH", required=True))
    if not borough_geojson_path.exists():
        raise FileNotFoundError(f"GeoJSON/Shapefile not found: {borough_geojson_path}")

    fully_qualified_table = f"{database}.{schema}.{table_name}"

    print("Connecting to Snowflake...")
    connection = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    cursor = connection.cursor()

    try:
        # Ensure update flag column exists (robust check via INFORMATION_SCHEMA)
        print(f"Ensuring column {UPDATE_FLAG_COLUMN} exists…")
        exists_sql = f"""
        SELECT 1
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{database}'
        AND TABLE_SCHEMA  = '{schema}'
        AND TABLE_NAME    = '{table_name}'
        AND UPPER(COLUMN_NAME) = UPPER('{UPDATE_FLAG_COLUMN}')
        LIMIT 1
        """
        exists = pd.read_sql(exists_sql, connection)

        if exists.empty:
            cursor.execute(
                f'ALTER TABLE {fully_qualified_table} ADD COLUMN "{UPDATE_FLAG_COLUMN}" BOOLEAN DEFAULT FALSE'
            )
            print(f'Added column "{UPDATE_FLAG_COLUMN}".')
        else:
            print(f'Column "{UPDATE_FLAG_COLUMN}" already exists. Skipping add.')


        #Pull candidate rows where BOROUGH but LATITUDE AND LONGITUDE are NOTNULL
        select_query = f"""
            SELECT {PRIMARY_KEY_COLUMN}, BOROUGH, LATITUDE, LONGITUDE
            FROM {fully_qualified_table}
            WHERE BOROUGH IS NULL
              AND LATITUDE IS NOT NULL
              AND LONGITUDE IS NOT NULL
        """
        print("Fetching candidate rows from Snowflake...")
        candidate_dataframe = pd.read_sql(select_query, connection)
        print(f"Candidate rows fetched: {len(candidate_dataframe):,}")
        if candidate_dataframe.empty:
            print("No rows found to update. Exiting.")
            return

        #Load borough polygons
        print(f"Loading borough polygons from: {borough_geojson_path}")
        borough_polygons = gpd.read_file(borough_geojson_path)

        #Ensure polygons are in WGS84 (latitude/longitude) format
        if borough_polygons.crs is None or str(borough_polygons.crs).lower() != "epsg:4326":
            borough_polygons = borough_polygons.to_crs("EPSG:4326")

        #Determine if borough name column exists
        possible_borough_name_columns = ["BoroName", "borough", "BoroughName", "BORONAME", "Boro_Name"]
        borough_name_column = next((col for col in possible_borough_name_columns if col in borough_polygons.columns), None)
        if borough_name_column is None:
            raise ValueError(f"Could not find a borough name column in {borough_geojson_path}. "
                             f"Available columns: {list(borough_polygons.columns)}")

        #Build a GeoDataFrame of collision points
        print("Building GeoDataFrame of collision points...")
        point_geodataframe = gpd.GeoDataFrame(
            candidate_dataframe,
            geometry=[Point(xy) for xy in zip(candidate_dataframe["LONGITUDE"], candidate_dataframe["LATITUDE"])],
            crs="EPSG:4326"
        )

        #Point-in-polygon join to assign boroughs
        print("Performing spatial join (which borough each point falls within)...")
        joined_dataframe = gpd.sjoin(
            point_geodataframe,
            borough_polygons[[borough_name_column, "geometry"]],
            how="left",
            predicate="within"
        )

        #Keep only successfully matched rows
        matched_updates = joined_dataframe[[PRIMARY_KEY_COLUMN, borough_name_column]].dropna(subset=[borough_name_column]).copy()
        matched_updates.rename(columns={borough_name_column: "INFERRED_BOROUGH"}, inplace=True)
        matched_updates["INFERRED_BOROUGH"] = matched_updates["INFERRED_BOROUGH"].astype(str).str.strip()

        #Deduplicate by primary key to satisfy MERGE requirements
        duplicate_count = matched_updates.duplicated(subset=[PRIMARY_KEY_COLUMN]).sum()
        if duplicate_count:
            print(f"Found {duplicate_count:,} duplicate {PRIMARY_KEY_COLUMN} rows in updates; keeping first per id.")
        matched_updates = (
            matched_updates
            .sort_values([PRIMARY_KEY_COLUMN, "INFERRED_BOROUGH"])
            .drop_duplicates(subset=[PRIMARY_KEY_COLUMN], keep="first")
            .reset_index(drop=True)
        )
        print(f"Rows after de-duplication: {len(matched_updates):,}")

        print(f"Rows with inferred borough: {len(matched_updates):,}")
        if matched_updates.empty:
            print("No matches found inside borough polygons. Exiting.")
            return

        #Upload inferred boroughs to a temporary table
        temporary_table = "TEMP_BOROUGH_UPDATES"
        print(f"Creating temporary table {temporary_table} and uploading inferred boroughs...")
        cursor.execute(f"CREATE OR REPLACE TEMP TABLE {temporary_table} ({PRIMARY_KEY_COLUMN} NUMBER, INFERRED_BOROUGH STRING)")

        upload_dataframe = matched_updates[[PRIMARY_KEY_COLUMN, "INFERRED_BOROUGH"]].copy()
        upload_dataframe[PRIMARY_KEY_COLUMN] = pd.to_numeric(upload_dataframe[PRIMARY_KEY_COLUMN], errors="coerce").astype("Int64")
        upload_dataframe = upload_dataframe.reset_index(drop=True)  # ensure a standard RangeIndex for write_pandas

        success, num_chunks, num_rows, _ = write_pandas(
            conn=connection,
            df=upload_dataframe,
            table_name=temporary_table,
            quote_identifiers=False
        )
        print(f"Uploaded {num_rows:,} rows in {num_chunks} chunk(s). Success={success}")

        #Merge updates into curated table
        print("Merging inferred boroughs into main table...")
        merge_query = f"""
            MERGE INTO {fully_qualified_table} AS target
            USING {temporary_table} AS source
              ON target.{PRIMARY_KEY_COLUMN} = source.{PRIMARY_KEY_COLUMN}
            WHEN MATCHED AND target.BOROUGH IS NULL THEN UPDATE SET
              target.BOROUGH = source.INFERRED_BOROUGH,
              target.{UPDATE_FLAG_COLUMN} = TRUE
        """
        cursor.execute(merge_query)
        print("Merge complete.")

        #Summarize results
        summary_query = f"""
            SELECT 
              COUNT(*) AS total_rows,
              SUM(CASE WHEN BOROUGH IS NULL THEN 1 ELSE 0 END) AS remaining_null_boroughs,
              SUM(CASE WHEN {UPDATE_FLAG_COLUMN} THEN 1 ELSE 0 END) AS rows_flagged_true
            FROM {fully_qualified_table}
        """
        summary = pd.read_sql(summary_query, connection)
        print("\nPost-merge summary:")
        print(summary.to_string(index=False))

    finally:
        cursor.close()
        connection.close()
        print("Snowflake connection closed.")

if __name__ == "__main__":
    main()