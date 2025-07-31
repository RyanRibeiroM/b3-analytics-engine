import os
import pandas as pd
import boto3
from datetime import datetime
import time


def get_env_variables():
    csv_path = os.getenv("SOURCE_FILE_PATH", "/app/data/COTAHIST_A2009_to_A2020_P.csv")
    bucket = os.getenv("MINIO_RAW_BUCKET", "raw")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    return csv_path, bucket, minio_endpoint, access_key, secret_key


def get_date_partition():
    today = datetime.today()
    return today.strftime("%Y/%m/%d")


def init_s3_client(endpoint, access_key, secret_key):
    while True:
        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
            s3_client.list_buckets()
            return s3_client
        except Exception as e:
            print(
                f"Erro ao conectar ao MinIO: {e}. Tentando novamente em 5 segundos..."
            )
            time.sleep(5)


def send_file_to_minio(s3_client, bucket, object_key, file_path):
    try:
        with open(file_path, "rb") as f:
            s3_client.put_object(Bucket=bucket, Key=object_key, Body=f)
    except Exception as e:
        print(f"Erro ao enviar {object_key} para MinIO: {e}")


def process_and_save_temp_files(csv_path):
    try:
        df = pd.read_csv(csv_path, encoding="latin-1", sep=",")
    except Exception as e:
        print(f"Erro fatal ao ler o arquivo {csv_path}: {e}")
        return None, None

    df_filtered = df[df["CODBDI"] == 2].copy()

    if df_filtered.empty:
        print("Nenhuma linha de cotação de ações encontrada após o filtro.")
        return None, None

    try:
        df_filtered["DATPRE"] = pd.to_datetime(df_filtered["DATPRE"], format="%Y-%m-%d")
    except Exception as e:
        print(f"Erro ao converter a coluna de data 'DATPRE': {e}")
        return None, None

    numeric_cols = ["PREABE", "PREMAX", "PREMIN", "PREULT", "QUATOT", "VOLTOT"]
    for col in numeric_cols:
        df_filtered[col] = pd.to_numeric(df_filtered[col], errors="coerce")

    df_filtered.dropna(subset=numeric_cols, inplace=True)

    if df_filtered.empty:
        print(
            "Todas as linhas foram removidas durante a limpeza. Nenhum arquivo será gerado."
        )
        return None, None

    base_name = os.path.splitext(os.path.basename(csv_path))[0]
    temp_parquet = f"/tmp/{base_name}_processed.parquet"
    temp_csv = f"/tmp/{base_name}_processed.csv"

    df_filtered.to_parquet(temp_parquet, index=False)
    df_filtered.to_csv(temp_csv, index=False, sep=";")

    return temp_parquet, temp_csv


def main():
    csv_path, bucket, minio_endpoint, access_key, secret_key = get_env_variables()

    if not os.path.exists(csv_path):
        print(f"Arquivo CSV de origem não encontrado em: {csv_path}")
        return

    temp_parquet_path, temp_csv_path = process_and_save_temp_files(csv_path)

    if temp_parquet_path and temp_csv_path:
        s3_client = init_s3_client(minio_endpoint, access_key, secret_key)
        partition_path = get_date_partition()

        csv_key = f"kaggle/{partition_path}/{os.path.basename(temp_csv_path)}"
        send_file_to_minio(s3_client, bucket, csv_key, temp_csv_path)

        parquet_key = f"kaggle/{partition_path}/{os.path.basename(temp_parquet_path)}"
        send_file_to_minio(s3_client, bucket, parquet_key, temp_parquet_path)


if __name__ == "__main__":
    main()
