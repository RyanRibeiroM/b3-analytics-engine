import json 
from kafka import KafkaProducer
import time 

def create_producer(bootstrap_servers):
    producer = None
    retry_count = 0
    max_retries = 5

    while not producer and retry_count < max_retries:
        try: 
            producer = KafkaProducer(
                bootstrap_servers = bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                api_version=(0,10,1)
            )
        except Exception as e:
            retry_count += 1
            print(f"Erro ao conectar ao Kafka (tentativa {retry_count}/{max_retries}): {e}")
            time.sleep(5)

    return producer

def send_message(producer, topic, message):
    if not producer:
        print("Producer não inicializado. Mensagem não enviada.")
        return False

    try:
        producer.send(topic, value=message)
        producer.flush()
        return True
    except Exception as e:
        print(f"Erro ao enviar mensagem para o tópico '{topic}': {e}")
        return False