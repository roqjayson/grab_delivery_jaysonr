from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add your script directory to the path
sys.path.append('api_ingestion.py')

from api_ingestion import run_ingestion

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'api_ingestion_dag',
    default_args=default_args,
    description='A simple DAG to ingest data from an API',
    schedule_interval='@daily',  # Adjust the schedule as needed
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

run_ingestion_task = PythonOperator(
    task_id='run_api_ingestion',
    python_callable=run_ingestion,
    dag=dag,
)

run_ingestion_task
