DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'user') THEN

      CREATE ROLE "user" WITH LOGIN PASSWORD 'password';
   END IF;
END
$do$;

SELECT 'CREATE DATABASE stock_data'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'stock_data')\gexec
SELECT 'CREATE DATABASE b3_dw'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'b3_dw')\gexec

GRANT ALL PRIVILEGES ON DATABASE stock_data TO "user";
GRANT ALL PRIVILEGES ON DATABASE b3_dw TO "user";

\c stock_data;

CREATE TABLE IF NOT EXISTS yfinance_quotes (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    "open" REAL,
    high REAL,
    low REAL,
    "close" REAL,
    volume BIGINT,
    "timestamp" TIMESTAMPTZ NOT NULL,
    UNIQUE (symbol, "timestamp")
);

GRANT ALL PRIVILEGES ON TABLE yfinance_quotes TO "user";
GRANT USAGE, SELECT ON SEQUENCE yfinance_quotes_id_seq TO "user";