FROM python:3.9-slim

WORKDIR /app

COPY ../../include/common/ /app/common/ 

COPY ./ingestion_services/postgres_producer/main.py .
COPY ./ingestion_services/postgres_producer/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]