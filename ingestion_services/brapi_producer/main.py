import os
import time
import requests
from common import kafka_producer as kp


def get_env_variables():
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    brapi_token = os.getenv("BRAPI_TOKEN")
    topic_name = os.getenv("BRAPI_TOPIC", "brapi_stock_quotes")

    if not brapi_token:
        print("Variável de ambiente BRAPI_TOKEN não definida.")
        exit(1)
    return kafka_servers, brapi_token, topic_name


def fetch_stock_data(ticker, api_token):
    url = f"https://brapi.dev/api/quote/{ticker}"

    headers = {"Authorization": f"Bearer {api_token}"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data and data.get("results"):
            return data["results"][0]
        return None

    except requests.exceptions.RequestException as e:
        print(f"Falha ao buscar dados para {ticker}: {e}")
        return None


def main():
    kafka_servers, brapi_token, topic_name = get_env_variables()
    tickers_to_monitor = ["PETR4", "VALE3", "ITUB4"]
    producer = kp.create_producer(bootstrap_servers=[kafka_servers])

    if not producer:
        print("Saindo, não foi possível conectar ao Kafka.")
        return

    try:
        while True:
            for ticker in tickers_to_monitor:
                stock_data = fetch_stock_data(ticker, brapi_token)

                if stock_data:
                    kp.send_message(producer, topic_name, stock_data)

                time.sleep(5)

            time.sleep(30)

    except KeyboardInterrupt:
        print("Execução interrompida.")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
