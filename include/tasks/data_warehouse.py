import os
import json
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from datetime import datetime
from minio import Minio
from io import BytesIO

def calculate_metrics(df):
    df = df.sort_values(by=['symbol', 'date'])
    
    numeric_cols = ['close', 'volume', 'marketCap']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=numeric_cols, inplace=True)

    df['sma_5'] = df.groupby('symbol')['close'].transform(lambda x: x.rolling(window=5, min_periods=1).mean())
    df['sma_20'] = df.groupby('symbol')['close'].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
    
    daily_turnover = df['volume'] * df['close']
    df['turnover_ratio'] = (daily_turnover / df['marketCap'])

    metrics_cols = ['sma_5', 'sma_20', 'turnover_ratio']
    df[metrics_cols] = df[metrics_cols].fillna(0)
    
    return df

def execute_load_to_dw(**kwargs):
    PG_HOST = os.getenv("PG_DW_HOST", "postgres")
    PG_PORT = os.getenv("PG_DW_PORT", "5432")
    PG_DATABASE = os.getenv("PG_DW_DATABASE", "b3_dw")
    PG_USER = os.getenv("PG_DW_USER", "user")
    PG_PASSWORD = os.getenv("PG_DW_PASSWORD", "password")
    TABLE_NAME = "b3_analytics_data"
    DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    engine = create_engine(DB_URL)

    KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    ENRICHED_TOPIC = "enriched_stock_data"

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    LOADED_BUCKET = "loaded"

    consumer = KafkaConsumer(
        ENRICHED_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        consumer_timeout_ms=15000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id=f"dw-loader-group-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )
    new_records = [message.value for message in consumer]
    consumer.close()

    if not new_records:
        return
        
    df_new = pd.DataFrame(new_records)
    df_new.rename(columns={'processed_at': 'date'}, inplace=True)
    df_new['date'] = pd.to_datetime(df_new['date'])

    df_history = pd.DataFrame()
    try:
        query_history = f'SELECT * FROM {TABLE_NAME} WHERE date >= NOW() - INTERVAL \'40 days\''
        df_history = pd.read_sql(query_history, engine)
        df_history['date'] = pd.to_datetime(df_history['date'])
    except Exception:
        print("Tabela de analytics ainda não existe. Processando apenas com novos dados.")

    df_combined = pd.concat([df_history, df_new], ignore_index=True)
    df_combined.drop_duplicates(subset=['symbol', 'date'], keep='last', inplace=True)

    df_final = calculate_metrics(df_combined)

    df_final.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)

    try:
        minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        json_data = df_final.to_json(orient="records", indent=4, date_format='iso').encode('utf-8')
        file_name = f"enriched_data/final_analytics_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        minio_client.put_object(LOADED_BUCKET, file_name, data=BytesIO(json_data), length=len(json_data), content_type='application/json')
    except Exception as e:
        print(f"Erro ao salvar arquivo no MinIO: {e}")