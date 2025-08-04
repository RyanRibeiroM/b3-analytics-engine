import os
from io import BytesIO
import boto3
import pandas as pd
from sqlalchemy import create_engine, text

def get_latest_file_from_minio(s3_client, bucket, file_extension):

    try:
        response = s3_client.list_objects_v2(Bucket=bucket)
        if "Contents" not in response:
            return None
        
        all_files = [obj for obj in response["Contents"] if obj["Key"].endswith(file_extension)]
        if not all_files:
            return None
            
        latest_file = max(all_files, key=lambda x: x["LastModified"])
        return latest_file["Key"]
    except Exception as e:
        print(f"Erro ao listar arquivos do MinIO: {e}")
        raise

def execute_load_to_dw(**kwargs):

    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    LOADED_BUCKET = "loaded"

    PG_HOST = "postgres"
    PG_PORT = "5432"
    PG_DATABASE = "b3_dw"
    PG_USER = "user"
    PG_PASSWORD = "password"
    TABLE_NAME = "b3_analytics_data"
    
    print("Iniciando tarefa de carregamento para o Data Warehouse.")
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    print(f"Procurando pelo arquivo mais recente no bucket '{LOADED_BUCKET}'...")
    key = get_latest_file_from_minio(s3_client, LOADED_BUCKET, ".parquet")

    if not key:
        print("Nenhum arquivo processado encontrado no bucket 'loaded'. Encerrando a task.")
        return

    print(f"Arquivo encontrado: {key}. Baixando e lendo para o DataFrame...")
    obj = s3_client.get_object(Bucket=LOADED_BUCKET, Key=key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    print(f"Conectando ao banco de dados do DW ({PG_DATABASE})...")
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    )

    print(f"Carregando dados na tabela '{TABLE_NAME}' com a estratégia 'replace'...")

    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

    print("Carga para o Data Warehouse concluída com sucesso.")