from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import retrain_model 

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'maintenance_dag',
    default_args=default_args,
    description='Monthly maintenance DAG for model retraining',
    schedule_interval='@monthly',
)

retrain_task = PythonOperator(
    task_id='retrain_model_task',
    python_callable=retrain_model.retrain_model,
    dag=dag,
)

# Set the task dependencies
retrain_task

