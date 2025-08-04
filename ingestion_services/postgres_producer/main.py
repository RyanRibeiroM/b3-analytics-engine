import os
import time
import json
from kafka import KafkaProducer
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

def get_env_variables():
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    yfinance_topic = os.getenv("YFINANCE_TOPIC")
    postgres_host = os.getenv("POSTGRES_HOST")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    return (
        kafka_brokers,
        yfinance_topic,
        postgres_host,
        postgres_db,
        postgres_user,
        postgres_password,
    )

def main():
    print(">>> [LOG] Iniciando o serviço postgres_producer...")
    (
        kafka_brokers,
        yfinance_topic,
        postgres_host,
        postgres_db,
        postgres_user,
        postgres_password,
    ) = get_env_variables()

    print(f">>> [LOG] Tentando conectar ao Kafka em: {kafka_brokers}")
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_brokers.split(','),
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
            print(">>> [LOG] Conexão com o Kafka estabelecida com SUCESSO!")
        except Exception as e:
            print(f">>> [ERRO] Falha ao conectar ao Kafka: {e}. Tentando novamente em 10 segundos...")
            time.sleep(10)

    db_url = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}/{postgres_db}"
    print(f">>> [LOG] Tentando conectar à base de dados Postgres em: {postgres_host}")
    engine = None
    while not engine:
        try:
            engine = create_engine(db_url)
            with engine.connect() as connection:
                print(">>> [LOG] Conexão com o Postgres estabelecida com SUCESSO!")
        except Exception as e:
            print(f">>> [ERRO] Falha ao conectar ao Postgres: {e}. Tentando novamente em 10 segundos...")
            time.sleep(10)

    last_timestamp = datetime.now() - timedelta(days=365 * 10) 

    print(">>> [LOG] Entrando no loop principal para ler dados do Postgres...")
    try:
        while True:
            with engine.connect() as connection:
                query = text(
                    "SELECT symbol, open, high, low, close, volume, timestamp "
                    "FROM yfinance_quotes WHERE timestamp > :last_ts ORDER BY timestamp ASC"
                )

                result = connection.execute(query, {"last_ts": last_timestamp})
                rows = result.fetchall()

                if rows:
                    print(f">>> [LOG] Encontradas {len(rows)} novas linhas na base de dados.")
                    for row in rows:
                        row_dict = dict(row._mapping)
                        producer.send(yfinance_topic, value=row_dict)

                    last_timestamp = rows[-1]._mapping['timestamp']
                    producer.flush()
                    print(f">>> [LOG] Lote enviado. Último timestamp processado: {last_timestamp}")
                else:
                    print(">>> [LOG] Nenhuma linha nova encontrada. A aguardar...")

            time.sleep(60)
    except Exception as e:
        print(f">>> [ERRO] Erro inesperado no loop principal: {e}")
    finally:
        if producer:
            producer.close()
        if engine:
            engine.dispose()
        print(">>> [LOG] Serviço postgres_producer finalizado.")

if __name__ == "__main__":
    main()