import os
import time
import yfinance as yf
from common import kafka_producer as kp

def get_env_variables():
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic_name = os.getenv('YFINANCE_TOPIC', 'yfinance_stock_quotes')
    return kafka_servers, topic_name

def fetch_yfinance_data(ticker_symbol):
    try:
        ticker_data = yf.Ticker(ticker_symbol)
        info = ticker_data.history(period="2d")
        
        if not info.empty:
            last_quote = info.iloc[-1]
            formatted_data = {
                'symbol': ticker_symbol,
                'open': last_quote.get('Open'),
                'high': last_quote.get('High'),
                'low': last_quote.get('Low'),
                'close': last_quote.get('Close'),
                'volume': int(last_quote.get('Volume')),
                'timestamp': last_quote.name.isoformat()
            }
            return formatted_data
        else:
            print(f"Nenhum dado histórico encontrado para {ticker_symbol}")
            return None
            
    except Exception as e:
        print(f"Falha ao buscar dados para {ticker_symbol}: {e}")
        return None

def main():
    kafka_servers, topic_name = get_env_variables()
    tickers_to_monitor = ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA']
    producer = kp.create_producer(bootstrap_servers=[kafka_servers])
    
    if not producer:
        print("Saindo, não foi possível conectar ao Kafka.")
        return

    try:
        while True:
            for ticker in tickers_to_monitor:
                stock_data = fetch_yfinance_data(ticker)
                
                if stock_data:
                    kp.send_message(producer, topic_name, stock_data)
            
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("Execução interrompida.")
    finally:
        if producer:
            producer.close()

if __name__ == '__main__':
    main()
