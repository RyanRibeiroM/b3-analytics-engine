import os
import time
from io import BytesIO
import boto3
import pandas as pd
from sqlalchemy import create_engine

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
LOADED_BUCKET = os.getenv("MINIO_LOADED_BUCKET", "loaded")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DATABASE = os.getenv("POSTGRES_DB", "b3_dw")
PG_USER = os.getenv("POSTGRES_USER", "user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

TABLE_NAME = "b3_analytics_data"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)

last_file = None
while True:
    res = s3.list_objects_v2(Bucket=LOADED_BUCKET, Prefix="")
    if "Contents" in res:
        parquet_files = [obj for obj in res["Contents"] if obj["Key"].endswith(".parquet")]
        if parquet_files:
            latest = max(parquet_files, key=lambda x: x["LastModified"])
            key = latest["Key"]

            if key != last_file:
                last_file = key

                obj = s3.get_object(Bucket=LOADED_BUCKET, Key=key)
                df = pd.read_parquet(BytesIO(obj["Body"].read()))

                df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

    else:
        print("Nenhum arquivo ainda em loaded/...")

    time.sleep(30)
