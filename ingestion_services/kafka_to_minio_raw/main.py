import os
import json
from kafka import KafkaConsumer
import boto3
from datetime import datetime
import time


def get_env_variables():
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092").split(",")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_raw_bucket = os.getenv("MINIO_RAW_BUCKET", "raw")

    return (
        kafka_brokers,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        minio_raw_bucket,
    )


def create_s3_client(minio_endpoint, minio_access_key, minio_secret_key):
    while True:
        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=minio_endpoint,
                aws_access_key_id=minio_access_key,
                aws_secret_access_key=minio_secret_key,
                config=boto3.session.Config(signature_version="s3v4"),
            )
            s3_client.list_buckets()
            return s3_client
        except Exception as e:
            print(
                f"Erro ao conectar ao MinIO: {e}. Tentando novamente em 5 segundos..."
            )
            time.sleep(5)


def write_to_minio(s3_client, bucket_name, object_key, data):
    try:
        s3_client.put_object(
            Bucket=bucket_name, Key=object_key, Body=data.encode("utf-8")
        )
        return True
    except Exception as e:
        print(f"Erro ao salvar em MinIO para o objeto '{object_key}': {e}")
        return False


def main():
    (
        kafka_brokers,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        minio_raw_bucket,
    ) = get_env_variables()

    s3_client = create_s3_client(minio_endpoint, minio_access_key, minio_secret_key)

    kafka_topics = ["brapi_stock_quotes", "postgres_stock_quotes"]
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                *kafka_topics,
                bootstrap_servers=kafka_brokers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="minio-raw-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
        except Exception as e:
            print(
                f"Erro ao conectar ao Kafka: {e}. Tentando novamente em 5 segundos..."
            )
            time.sleep(5)

    try:
        for message in consumer:
            record_value = message.value
            topic = message.topic

            current_date = datetime.now()
            path_prefix = current_date.strftime("%Y/%m/%d")

            object_key = (
                f"{topic}/{path_prefix}/{message.timestamp}_{message.offset}.json"
            )

            print(f"Recebido da Tópico: {topic}, Offset: {message.offset}")
            write_to_minio(
                s3_client,
                minio_raw_bucket,
                object_key,
                json.dumps(record_value, default=str),
            )

    except KeyboardInterrupt:
        print("Execução interrompida pelo usuário.")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        if consumer:
            consumer.close()


if __name__ == "__main__":
    main()
