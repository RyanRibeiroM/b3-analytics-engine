import os
import pandas as pd
import boto3
from datetime import datetime
from io import BytesIO

def execute_batch_ingestion(**kwargs):
    
    csv_path = "/opt/airflow/ingestion_data/COTAHIST_A2009_to_A2020_P.csv"
    bucket = "raw"
    minio_endpoint = "http://minio:9000"
    access_key = "minioadmin"
    secret_key = "minioadmin"
    
    print(f"Iniciando ingestão do arquivo: {csv_path}")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo de origem não encontrado em: {csv_path}")

    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    df = pd.read_csv(csv_path, encoding="latin-1", sep=",")
    
    df_filtered = df[df["CODBDI"] == 2].copy()

    if df_filtered.empty:
        return

    numeric_cols = ["PREABE", "PREMAX", "PREMIN", "PREULT", "QUATOT", "VOLTOT"]
    for col in numeric_cols:
        df_filtered[col] = pd.to_numeric(df_filtered[col], errors="coerce")
    
    df_filtered.dropna(subset=numeric_cols, inplace=True)
    
    try:
        df_filtered["DATPRE"] = pd.to_datetime(df_filtered["DATPRE"], format="%Y-%m-%d")
    except Exception as e:
        print(f"Erro ao converter a coluna de data 'DATPRE': {e}")
        raise

    if df_filtered.empty:
        print("Todas as linhas foram removidas durante a limpeza. Nenhum arquivo será gerado.")
        return

    partition_path = datetime.today().strftime("%Y%m%d")
    base_name = os.path.splitext(os.path.basename(csv_path))[0]
    
    parquet_key = f"kaggle/{partition_path}/{base_name}_historical.parquet"
    
    out_buffer = BytesIO()
    df_filtered.to_parquet(out_buffer, index=False)
    out_buffer.seek(0)
    
    s3_client.put_object(Bucket=bucket, Key=parquet_key, Body=out_buffer.read())