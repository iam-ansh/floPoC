import json
import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")  
POSTGRES_PORT = os.getenv("POSTGRES_PORT")  

# Connect to Postgres (update credentials to match your container setup)
conn = psycopg2.connect(
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,  # or "db" if using docker-compose service name
    port=POSTGRES_PORT
)
cursor = conn.cursor()

# Create table if not exists
create_table = """
CREATE TABLE IF NOT EXISTS argo_data_test (
    id SERIAL PRIMARY KEY,
    title TEXT,
    institution TEXT,
    source TEXT,
    history TEXT,
    references_text TEXT,
    user_manual_version TEXT,
    conventions TEXT,
    featureType TEXT,
    temperature_min DOUBLE PRECISION,
    temperature_max DOUBLE PRECISION,
    pressure_min DOUBLE PRECISION,
    pressure_max DOUBLE PRECISION,
    salinity_min DOUBLE PRECISION,
    salinity_max DOUBLE PRECISION,
    time_start TIMESTAMP,
    time_end TIMESTAMP
);
"""
cursor.execute(create_table)

# Load JSON file
json_folder = Path("json_test")
for file in json_folder.glob("*.json"):
    with open(file, "r") as f:
        data = json.load(f)

        insert_query = """
        INSERT INTO argo_data_test (
            title, institution, source, history, references_text,
            user_manual_version, conventions, featureType,
            temperature_min, temperature_max,
            pressure_min, pressure_max,
            salinity_min, salinity_max,
            time_start, time_end
        ) VALUES (
            %(title)s, %(institution)s, %(source)s, %(history)s, %(references)s,
            %(user_manual_version)s, %(Conventions)s, %(featureType)s,
            %(temperature_min)s, %(temperature_max)s,
            %(pressure_min)s, %(pressure_max)s,
            %(salinity_min)s, %(salinity_max)s,
            %(time_start)s, %(time_end)s
        );
        """
        cursor.execute(insert_query, data)
    print(f"{file}done")

conn.commit()
cursor.close()
conn.close()
