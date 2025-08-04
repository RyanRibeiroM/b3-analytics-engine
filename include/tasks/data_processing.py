import os
import pandas as pd
import boto3
from io import BytesIO
import json
from datetime import datetime


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    )

def get_all_s3_objects(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    all_objects = []
    for page in pages:
        if "Contents" in page:
            all_objects.extend(page["Contents"])
    return all_objects

def save_df_to_minio(s3_client, df, bucket, key):
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    out_buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.read())
    print(f"DataFrame salvo com sucesso em: {bucket}/{key}")


def execute_data_processing(**kwargs):
    s3_client = get_s3_client()
    raw_bucket = "raw"
    loaded_bucket = "loaded"
    execution_date_str = kwargs['ds_nodash']

    print("Carregando dados históricos do MinIO...")
    historical_prefix = "kaggle/"
    historical_files = get_all_s3_objects(s3_client, raw_bucket, historical_prefix)
    if not historical_files:
        raise FileNotFoundError("Nenhum arquivo histórico encontrado em raw/kaggle/. Execute a DAG histórica primeiro.")

    latest_historical_file = max(historical_files, key=lambda x: x["LastModified"])
    obj = s3_client.get_object(Bucket=raw_bucket, Key=latest_historical_file["Key"])
    df_historical = pd.read_parquet(BytesIO(obj["Body"].read()))
    print(f"Arquivo histórico carregado: {latest_historical_file['Key']}")

    print("Carregando novos dados incrementais do MinIO...")
    date_path = kwargs['ds'].replace('-', '/')

    all_new_records = []
    for source in ["brapi_stock_quotes", "postgres_stock_quotes"]:
        prefix = f"{source}/{date_path}/"
        new_files = get_all_s3_objects(s3_client, raw_bucket, prefix)
        if not new_files:
            print(f"Nenhum arquivo novo encontrado para a fonte '{source}' na data {date_path}.")
            continue

        print(f"Encontrados {len(new_files)} arquivos para a fonte '{source}'.")
        for file in new_files:
            obj = s3_client.get_object(Bucket=raw_bucket, Key=file["Key"])
            content = obj["Body"].read().decode('utf-8')
            all_new_records.append(json.loads(content))

    if not all_new_records:
        print("Nenhum dado incremental novo para processar. Encerrando a task.")
        return

    df_new_data = pd.DataFrame(all_new_records)
    print(f"Total de {len(df_new_data)} novos registos carregados.")

    print("Iniciando limpeza e transformação dos dados...")

    df_historical.rename(columns={
        "DATPRE": "date", "CODNEG": "symbol", "PREABE": "open",
        "PREMAX": "high", "PREMIN": "low", "PREULT": "close", "QUATOT": "volume"
    }, inplace=True)
    df_historical['date'] = pd.to_datetime(df_historical['date'])
    df_historical['symbol'] = df_historical['symbol'].str.strip() + ".SA"
    df_historical = df_historical[["date", "symbol", "open", "high", "low", "close", "volume"]]

    df_new_data.rename(columns={"timestamp": "date"}, inplace=True)
    df_new_data['date'] = pd.to_datetime(df_new_data['date'], utc=True).dt.tz_convert('America/Sao_Paulo').dt.date
    df_new_data['date'] = pd.to_datetime(df_new_data['date'])
    df_new_data['symbol'] = df_new_data['symbol'].str.strip() + ".SA"
    df_new_data = df_new_data.get(["date", "symbol", "open", "high", "low", "close", "volume"], df_new_data)

    print("Unindo dados históricos e novos...")
    df_full = pd.concat([df_historical, df_new_data], ignore_index=True)
    df_full.drop_duplicates(subset=['symbol', 'date'], keep='last', inplace=True)
    df_full.sort_values(by=['symbol', 'date'], inplace=True)
    df_full.reset_index(drop=True, inplace=True)

    print("Calculando novas métricas de negócio...")
    df_full['daily_return'] = df_full.groupby('symbol')['close'].pct_change()
    df_full['sma_5_days'] = df_full.groupby('symbol')['close'].transform(lambda x: x.rolling(window=5, min_periods=1).mean())
    df_full['sma_20_days'] = df_full.groupby('symbol')['close'].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
    df_full['volatility_5_days'] = df_full.groupby('symbol')['daily_return'].transform(lambda x: x.rolling(window=5, min_periods=1).std())
    df_full['volatility_20_days'] = df_full.groupby('symbol')['daily_return'].transform(lambda x: x.rolling(window=20, min_periods=1).std())

    df_full.dropna(inplace=True)

    processing_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_key = f"{execution_date_str}/b3_final_processed_{processing_timestamp}.parquet"
    save_df_to_minio(s3_client, df_full, loaded_bucket, final_key)

    print("Task de processamento concluída com sucesso.")