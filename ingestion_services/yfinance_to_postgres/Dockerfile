FROM python:3.9-slim

WORKDIR /app

COPY ./common/ /app/common/

COPY ./ingestion_services/yfinance_to_postgres/main.py .
COPY ./ingestion_services/yfinance_to_postgres/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
