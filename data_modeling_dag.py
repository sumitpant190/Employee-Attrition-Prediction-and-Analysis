from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

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
    'data_modeling',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False
)

# Define the task for EDA
run_eda = BashOperator(
    task_id='run_eda',
    bash_command='papermill /mnt/f/attrition_etl/attrition_etl.ipynb /mnt/f/attrition_etl/eda/eda_output.ipynb -p task eda',
    dag=dag,
)

# Define the task for Feature Engineering
run_feature_engineering = BashOperator(
    task_id='run_feature_engineering',
    bash_command='papermill /mnt/f/attrition_etl/attrition_etl.ipynb /mnt/f/attrition_etl/eda/feature_engineering_output.ipynb -p task feature_engineering',
    dag=dag,
)

# Define the task for Model Training
run_model_training = BashOperator(
    task_id='run_model_training',
    bash_command = 'papermill /mnt/f/attrition_etl/attrition_etl.ipynb /mnt/f/attrition_etl/eda/model_training_output.ipynb -p task model_training',
    dag = dag
)

run_feature_engineering
