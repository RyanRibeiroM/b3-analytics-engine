import os
import time
import yfinance as yf
import psycopg2
from psycopg2 import sql

def get_env_variables():
    """Busca as variáveis de ambiente para a conexão com o PostgreSQL."""
    db_host = os.getenv('POSTGRES_HOST', 'localhost')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_name = os.getenv('POSTGRES_DB', 'stock_data')
    db_user = os.getenv('POSTGRES_USER', 'user')
    db_password = os.getenv('POSTGRES_PASSWORD', 'password')
    
    return db_host, db_port, db_name, db_user, db_password

def create_pg_connection(db_host, db_port, db_name, db_user, db_password):
    """Cria e retorna uma conexão com o banco de dados PostgreSQL."""
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
            print("Conexão com o PostgreSQL (yfinance) estabelecida com sucesso.")
        except psycopg2.OperationalError as e:
            print(f"Erro ao conectar ao PostgreSQL: {e}. Tentando novamente em 5 segundos...")
            time.sleep(5)
    return conn

def create_table_if_not_exists(conn):
    """Cria a tabela para os dados do yfinance se ela não existir."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS yfinance_quotes (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(15),
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT,
                timestamp TIMESTAMP,
                ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        print("Tabela 'yfinance_quotes' verificada/criada.")

def fetch_yfinance_data(ticker_symbol):
    """Busca os dados mais recentes de um ticker usando a biblioteca yfinance."""
    try:
        ticker_data = yf.Ticker(ticker_symbol)
        info = ticker_data.history(period="2d")
        
        if not info.empty:
            last_quote = info.iloc[-1]
            return {
                'symbol': ticker_symbol,
                'open': float(last_quote.get('Open')),
                'high': float(last_quote.get('High')),
                'low': float(last_quote.get('Low')),
                'close': float(last_quote.get('Close')),
                'volume': int(last_quote.get('Volume')),
                'timestamp': last_quote.name.to_pydatetime()
            }

        else:
            print(f"Nenhum dado histórico encontrado para {ticker_symbol}")
            return None
            
    except Exception as e:
        print(f"Falha ao buscar dados para {ticker_symbol}: {e}")
        return None

def insert_stock_data(conn, stock_data):
    """Insere os dados de ações na tabela do PostgreSQL."""
    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO yfinance_quotes (symbol, open, high, low, close, volume, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """)
        try:
            cur.execute(insert_query, (
                stock_data['symbol'],
                stock_data['open'],
                stock_data['high'],
                stock_data['low'],
                stock_data['close'],
                stock_data['volume'],
                stock_data['timestamp']
            ))
            conn.commit()
            print(f"Dados inseridos para o ticker: {stock_data['symbol']} em {stock_data['timestamp']}")
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
            conn.rollback()

def main():
    db_host, db_port, db_name, db_user, db_password = get_env_variables()
    tickers_to_monitor = ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA']
    
    conn = create_pg_connection(db_host, db_port, db_name, db_user, db_password)
    create_table_if_not_exists(conn)

    try:
        while True:
            for ticker in tickers_to_monitor:
                stock_data = fetch_yfinance_data(ticker)
                
                if stock_data:
                    insert_stock_data(conn, stock_data)
            
            # Espera 30 segundos antes de buscar novos dados
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("Execução interrompida.")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()