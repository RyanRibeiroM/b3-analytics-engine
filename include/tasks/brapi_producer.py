import os
import requests
from include.common import kafka_producer as kp

def execute_brapi_producer(**kwargs):

    kafka_servers = "kafka:29092"
    brapi_token = os.getenv("BRAPI_TOKEN") 
    topic_name = "brapi_stock_quotes"
    tickers_to_monitor = ["PETR4", "VALE3", "ITUB4"]

    if not brapi_token:
        raise ValueError("A variável de ambiente BRAPI_TOKEN não foi definida no ambiente do Airflow.")

    producer = kp.create_producer(bootstrap_servers=[kafka_servers])
    if not producer:
        raise ConnectionError("Não foi possível conectar ao Kafka. A tarefa irá falhar.")

    for ticker in tickers_to_monitor:
        url = f"https://brapi.dev/api/quote/{ticker}"
        headers = {"Authorization": f"Bearer {brapi_token}"}
        
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if data and data.get("results"):
                stock_data = data["results"][0]
                success = kp.send_message(producer, topic_name, stock_data)
            else:
                print(f"Resposta da API para o ticker '{ticker}' não continha 'results'.")

        except requests.exceptions.RequestException as e:
            print(f"Falha na requisição HTTP para o ticker {ticker}: {e}")
        except Exception as e:
            print(f"Ocorreu um erro inesperado ao processar o ticker {ticker}: {e}")
    
    producer.close()
