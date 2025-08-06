import sys
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from datetime import timedelta

sys.path.insert(0, '/opt/airflow')

from include.tasks import (
    batch_ingestion, 
    brapi_producer, 
    yfinance_to_postgres, 
    data_processing, 
    data_warehouse
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='1_historical_load_dag',
    start_date=days_ago(1),
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['market-data', 'historical', 'setup'],
    doc_md="""
    ### DAG de Carga Histórica
    """
)
def historical_load_dag():
    historical_task = PythonOperator(
        task_id='execute_historical_batch_ingestion',
        python_callable=batch_ingestion.execute_batch_ingestion
    )

@dag(
    dag_id='2_incremental_market_data_dag',
    start_date=days_ago(1),
    default_args=default_args,
    schedule='* * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['market-data', 'streaming-enrichment'],
    doc_md="""
    ### Pipeline de Dados de Mercado via Streaming
    """
)
def incremental_market_data_dag():

    ingest_brapi_task = PythonOperator(
        task_id='ingest_brapi_data_to_kafka',
        python_callable=brapi_producer.execute_brapi_producer
    )

    ingest_yfinance_task = PythonOperator(
        task_id='ingest_yfinance_data_to_postgres',
        python_callable=yfinance_to_postgres.execute_yfinance_to_postgres
    )

    process_data_task = PythonOperator(
        task_id='process_and_enrich_market_data',
        python_callable=data_processing.execute_data_processing
    )

    load_to_dw_task = PythonOperator(
        task_id='load_processed_data_to_dw',
        python_callable=data_warehouse.execute_load_to_dw
    )

    chain(
        [ingest_brapi_task, ingest_yfinance_task],
        process_data_task,
        load_to_dw_task
    )

historical_load_dag_instance = historical_load_dag()
incremental_market_data_dag_instance = incremental_market_data_dag()