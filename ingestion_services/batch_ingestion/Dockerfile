FROM python:3.9-slim

WORKDIR /app

COPY ./ingestion_services/batch_ingestion/main.py .

COPY ./ingestion_services/batch_ingestion/COTAHIST_A2009_to_A2020_P.csv /app/data/COTAHIST_A2009_to_A2020_P.csv
COPY ./ingestion_services/batch_ingestion/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
