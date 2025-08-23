from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import os
import sys

# Add scripts directory to path so Airflow can find our script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../scripts'))

from fetch_stock_data import main as fetch_stock_data_main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='A pipeline to fetch and store stock market data',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['stock', 'data', 'pipeline'],
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

fetch_stock_data_task = PythonOperator(
    task_id='fetch_and_store_stock_data',
    python_callable=fetch_stock_data_main,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

start_task >> fetch_stock_data_task >> end_task