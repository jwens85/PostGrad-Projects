#Export 200K sample collision points to GeoJSON using Snowflake via SQLAlchemy
import os
from urllib.parse import quote_plus
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

#Configs
DATA_DIR = r"C:\Users\jwens\Desktop\PostGrad-Projects\NY_Motor_Vehicle_Collisions\Data"
OUTPUT_GEOJSON = os.path.join(DATA_DIR, "nyc_collision_points.geojson")
ROW_LIMIT = 200000
HOUR_FILTER = None
SAMPLE_FRAC = 1.0
REQUIRE_BOROUGH_NOT_NULL = False

#Load environment variables
def load_env():
    here = os.path.dirname(__file__)
    load_dotenv(dotenv_path=os.path.join(here, ".env"), override=False)

#Create SQLAlchemy engine for Snowflake
def snowflake_engine_from_env():
    required = [
        "SF_USER", "SF_PASSWORD", "SF_ACCOUNT", "SF_DATABASE",
        "SF_SCHEMA", "SF_WAREHOUSE", "SF_ROLE", "SF_TABLE"
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env var(s): {', '.join(missing)} in .env")
    url = (
        f"snowflake://{os.getenv('SF_USER')}:{quote_plus(os.getenv('SF_PASSWORD'))}"
        f"@{os.getenv('SF_ACCOUNT')}/{os.getenv('SF_DATABASE')}/{os.getenv('SF_SCHEMA')}"
        f"?warehouse={os.getenv('SF_WAREHOUSE')}&role={os.getenv('SF_ROLE')}"
    )
    return create_engine(url)

#Build SQL for the curated table with exact column names
def build_sql(table):
    where_clauses = [
        "LATITUDE IS NOT NULL",
        "LONGITUDE IS NOT NULL",
        '"CRASH TIME" IS NOT NULL'
    ]
    params = {}
    if HOUR_FILTER is not None:
        where_clauses.append('EXTRACT(HOUR FROM "CRASH TIME") = :h')
        params["h"] = int(HOUR_FILTER)
    if REQUIRE_BOROUGH_NOT_NULL:
        where_clauses.append("BOROUGH IS NOT NULL")
    where = " AND ".join(where_clauses)
    limit = f" LIMIT {int(ROW_LIMIT)}" if ROW_LIMIT else ""
    sql = f"""
        SELECT
            LATITUDE AS lat,
            LONGITUDE AS lon,
            EXTRACT(HOUR FROM "CRASH TIME") AS hour,
            BOROUGH AS borough
        FROM {table}
        WHERE {where}
        {limit}
    """
    return text(sql), params

def main():
    load_env()
    engine = snowflake_engine_from_env()
    table = os.getenv("SF_TABLE")

    sql, params = build_sql(table)
    print("Querying Snowflake...")
    df = pd.read_sql(sql, engine, params=params)
    print(f"Returned {len(df)} rows.")

    if df.empty:
        print("No rows found with current filters.")
        return

    if SAMPLE_FRAC < 1.0:
        df = df.sample(frac=SAMPLE_FRAC, random_state=42)

    #Create GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
        crs="EPSG:4326"
    )

    #Add geojson.io style properties
    gdf["marker-color"] = "#e31a1c"
    gdf["marker-size"] = "small"
    gdf["marker-symbol"] = "circle"

    os.makedirs(DATA_DIR, exist_ok=True)
    gdf.to_file(OUTPUT_GEOJSON, driver="GeoJSON")
    print(f"GeoJSON saved: {OUTPUT_GEOJSON}")

if __name__ == "__main__":
    main()
