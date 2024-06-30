# attrition_predictor_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 24),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'attrition_predictor_dag',
    default_args=default_args,
    description='Run Streamlit app for attrition prediction',
    schedule_interval=None,  # Define your schedule interval here
)

# Function to run the Streamlit app
def run_streamlit_app():
    # Change directory to where your Streamlit app is located
    os.chdir(r"/mnt/f/attrition_etl/")  # Update this path as per your WSL path
    os.system('streamlit run attrition_predictor_streamlit.py')

# Define the task to run the Streamlit app
run_streamlit_task = PythonOperator(
    task_id='run_streamlit_app',
    python_callable=run_streamlit_app,
    dag=dag,
)

# Set task dependencies if any
# task2.set_upstream(task1)
# task3.set_upstream(task2)

# You must explicitly tell Airflow to execute the task in your DAG
run_streamlit_task
