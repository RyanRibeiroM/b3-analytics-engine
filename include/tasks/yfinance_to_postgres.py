import os
import time
import yfinance as yf
import psycopg2
from psycopg2 import sql

def create_table_if_not_exists(conn):

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS yfinance_quotes (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(15),
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT,
                timestamp TIMESTAMP,
                ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, timestamp)
            );
            """
        )
        conn.commit()
        print("Tabela 'yfinance_quotes' verificada/criada com sucesso.")

def insert_stock_data(conn, stock_data):

    insert_query = sql.SQL(
        """
        INSERT INTO yfinance_quotes (symbol, open, high, low, close, volume, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
        """
    )
    with conn.cursor() as cur:
        try:
            cur.execute(
                insert_query,
                (
                    stock_data["symbol"],
                    stock_data["open"],
                    stock_data["high"],
                    stock_data["low"],
                    stock_data["close"],
                    stock_data["volume"],
                    stock_data["timestamp"],
                ),
            )
            conn.commit()
            if cur.rowcount > 0:
                print(f"Dados inseridos para: {stock_data['symbol']} em {stock_data['timestamp']}")
            else:
                print(f"Dados para {stock_data['symbol']} em {stock_data['timestamp']} já existiam. Ignorando.")
        except Exception as e:
            print(f"Erro ao inserir dados para {stock_data['symbol']}: {e}")
            conn.rollback()
            raise

def execute_yfinance_to_postgres(**kwargs):

    db_host = "postgres"
    db_port = "5432"
    db_name = "stock_data"
    db_user = "user"
    db_password = "password"
    tickers_to_monitor = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "^BVSP"]

    print("Conectando ao PostgreSQL...")
    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )
    
    create_table_if_not_exists(conn)

    print(f"Buscando dados para os tickers: {tickers_to_monitor}")
    for ticker in tickers_to_monitor:
        try:
            ticker_data = yf.Ticker(ticker)
            info = ticker_data.history(period="1d")

            if not info.empty:
                last_quote = info.iloc[-1]
                stock_data = {
                    "symbol": ticker,
                    "open": float(last_quote.get("Open")),
                    "high": float(last_quote.get("High")),
                    "low": float(last_quote.get("Low")),
                    "close": float(last_quote.get("Close")),
                    "volume": int(last_quote.get("Volume")),
                    "timestamp": last_quote.name.to_pydatetime(),
                }
                insert_stock_data(conn, stock_data)
            else:
                print(f"Nenhum dado histórico encontrado para {ticker} no período solicitado.")

        except Exception as e:
            print(f"Falha geral ao processar o ticker {ticker}: {e}")

    print("Fechando a conexão com o PostgreSQL.")
    conn.close()