import json
import os
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
from minio import Minio
from io import BytesIO

def create_kafka_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        auto_offset_reset='earliest',
        consumer_timeout_ms=15000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id=f"processing-group-{topic}-{datetime.now().isoformat()}"
    )

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

def create_minio_client():
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )

def execute_data_processing(**kwargs):
    brapi_consumer = create_kafka_consumer('brapi_stock_quotes')
    yfinance_consumer = create_kafka_consumer('postgres_stock_quotes')
    producer = create_kafka_producer()
    minio_client = create_minio_client()
    
    processing_bucket = "processing"

    if not minio_client.bucket_exists(processing_bucket):
        minio_client.make_bucket(processing_bucket)

    enriched_topic = 'enriched_stock_data'
    brapi_data = {}
    yfinance_data = {}

    for message in brapi_consumer:
        data = message.value
        if data and 'symbol' in data:
            brapi_data[data['symbol']] = data

    for message in yfinance_consumer:
        data = message.value
        if data and 'symbol' in data:
            symbol_normalized = data['symbol'].replace(".SA", "")
            yfinance_data[symbol_normalized] = data

    brapi_consumer.close()
    yfinance_consumer.close()

    enriched_count = 0
    for symbol, b_data in brapi_data.items():
        if symbol in yfinance_data:
            y_data = yfinance_data[symbol]
            
            close_price = y_data.get('close', 0)
            open_price = y_data.get('open', 0)
            
            enriched_message = {
                "symbol": symbol,
                "longName": b_data.get("longName"),
                "regularMarketPrice": b_data.get("regularMarketPrice"),
                "regularMarketChange": b_data.get("regularMarketChange"),
                "regularMarketChangePercent": b_data.get("regularMarketChangePercent"),
                "marketCap": b_data.get("marketCap"),
                "open": open_price,
                "high": y_data.get("high"),
                "low": y_data.get("low"),
                "close": close_price,
                "volume": y_data.get("volume"),
                "change_day": close_price - open_price if open_price and close_price else 0,
                "processed_at": datetime.now().isoformat()
            }
            
            producer.send(enriched_topic, value=enriched_message)

            try:
                json_bytes = json.dumps(enriched_message, default=str).encode('utf-8')
                object_name = f"{symbol}/{enriched_message['processed_at']}.json"
                
                minio_client.put_object(
                    bucket_name=processing_bucket, 
                    object_name=object_name, 
                    data=BytesIO(json_bytes), 
                    length=len(json_bytes), 
                    content_type='application/json'
                )
            except Exception as e:
                print(f"ERRO ao salvar o objeto '{object_name}' no MinIO: {e}")

            enriched_count += 1
    
    producer.flush()
    producer.close()
    
    if enriched_count == 0:
        print("Nenhum dado foi enriquecido. Verifique se os produtores estão enviando dados para os tópicos de origem.")

