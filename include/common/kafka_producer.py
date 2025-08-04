import json
from kafka import KafkaProducer
import time

def create_producer(bootstrap_servers, retries=5, delay=5):
    
    producer = None
    attempt = 0
    while attempt < retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                request_timeout_ms=30000 
            )
            print("Produtor Kafka conectado com sucesso.")
            return producer
        except Exception as e:
            attempt += 1
            print(f"Erro ao conectar ao Kafka (tentativa {attempt}/{retries}): {e}")
            if attempt < retries:
                print(f"Tentando novamente em {delay} segundos...")
                time.sleep(delay)
    
    print("Não foi possível criar o produtor Kafka após múltiplas tentativas.")
    return None

def send_message(producer, topic, message):

    if not producer:
        print("Erro: Produtor Kafka não inicializado. Mensagem não enviada.")
        return False

    try:
        future = producer.send(topic, value=message)
        record_metadata = future.get(timeout=10)
        return True
    except Exception as e:
        print(f"Erro ao enviar mensagem para o tópico '{topic}': {e}")
        return False