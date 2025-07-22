import os
import time
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from common import kafka_producer as kp
from decimal import Decimal

def get_env_variables():
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic_name = os.getenv('YFINANCE_TOPIC', 'postgres_stock_quotes')

    db_host = os.getenv('POSTGRES_HOST', 'localhost')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_name = os.getenv('POSTGRES_DB', 'stock_data')
    db_user = os.getenv('POSTGRES_USER', 'user')
    db_password = os.getenv('POSTGRES_PASSWORD', 'password')
    
    return kafka_servers, topic_name, db_host, db_port, db_name, db_user, db_password

def create_pg_connection(db_host, db_port, db_name, db_user, db_password):
    conn = None
    while not conn:
        try:
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                dbname=db_name,
                user=db_user,
                password=db_password
            )
            print("Conexão com o PostgreSQL (postgres_producer) estabelecida com sucesso.")
        except psycopg2.OperationalError as e:
            print(f"Erro ao conectar ao PostgreSQL: {e}. Tentando novamente em 5 segundos...")
            time.sleep(5)
    return conn

def default_json_serializer(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def main():
    kafka_servers, topic_name, db_host, db_port, db_name, db_user, db_password = get_env_variables()
    
    producer = kp.create_producer(bootstrap_servers=[kafka_servers])
    if not producer:
        print("Saindo, não foi possível conectar ao Kafka.")
        return
        
    conn = create_pg_connection(db_host, db_port, db_name, db_user, db_password)

    conn.autocommit = True

    last_processed_id = 0
    
    try:
        while True:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM yfinance_quotes WHERE id > %s ORDER BY id ASC", (last_processed_id,))
                records = cur.fetchall()
            
            if records:
                for record in records:
                    message_to_send = json.loads(json.dumps(record, default=default_json_serializer))
                    kp.send_message(producer, topic_name, message_to_send)
                    print(f"Enviado registro ID {record['id']} para o Kafka.")
                    last_processed_id = record['id']
            else:
                time.sleep(10)

    except KeyboardInterrupt:
        print("Execução interrompida.")
    finally:
        if producer:
            producer.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    main()