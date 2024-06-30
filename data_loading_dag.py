from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging

# Ensure logging is set up
logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def run_etl():
    from data_loading_etl import etl_postgres_to_postgres
    etl_postgres_to_postgres()

with DAG('data_loading_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    etl_task = PythonOperator(
        task_id='data_loading_warehouse_postgres',
        python_callable=run_etl
    )
