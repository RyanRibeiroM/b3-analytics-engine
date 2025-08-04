import sys

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from datetime import timedelta

sys.path.insert(0, '/opt/airflow')

from include.tasks import batch_ingestion, brapi_producer, yfinance_to_postgres, data_processing, data_warehouse, generate_dashboard 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
    Esta DAG deve ser executada uma vez para popular o Data Lake (MinIO)
    com os dados históricos da B3.
    Ela lê o arquivo CSV grande, o processa e salva o resultado em formato Parquet.
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
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    tags=['market-data', 'incremental'],
    doc_md="""
    ### Pipeline de Dados de Mercado Incremental
    Esta DAG orquestra o fluxo contínuo de dados:
    1.  **Ingestão**: Busca dados da API Brapi e yfinance em paralelo.
    2.  **Ponte**: Serviços externos (`kafka_to_minio_raw` e `postgres_producer`) movem os dados para o Data Lake e Kafka.
    3.  **Sensor**: Aguarda a chegada de novos dados da Brapi no MinIO.
    4.  **Processamento**: Se novos dados chegarem, a task de processamento é acionada.
    5.  **Carregamento**: O resultado final é carregado no Data Warehouse.
    6. **Dashboard**: Gera visuais para análise dos dados de mercado.
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

    wait_for_raw_brapi_data = S3KeySensor(
        task_id='wait_for_raw_brapi_data_in_minio',
        bucket_name='raw',
        bucket_key='brapi_stock_quotes/{{ execution_date.strftime("%Y/%m/%d") }}/*.json', 
        aws_conn_id='minio_conn', 
        wildcard_match=True,
        timeout=600,       
        poke_interval=30,  
        mode='poke'        
    )

    process_data_task = PythonOperator(
        task_id='process_and_enrich_market_data',
        python_callable=data_processing.execute_data_processing
    )

    load_to_dw_task = PythonOperator(
        task_id='load_processed_data_to_dw',
        python_callable=data_warehouse.execute_load_to_dw
    )

    generate_dashboard_task = PythonOperator(
        task_id='generate_dashboard_visuals',
        python_callable=generate_dashboard.execute_dashboard_generation
    )
    
    chain(
        [ingest_brapi_task, ingest_yfinance_task],
        wait_for_raw_brapi_data,
        process_data_task,
        load_to_dw_task,
        generate_dashboard_task
    )

historical_load_dag_instance = historical_load_dag()
incremental_market_data_dag_instance = incremental_market_data_dag()