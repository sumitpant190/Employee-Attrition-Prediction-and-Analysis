from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import data_cleaning_etl as etl

# Define your DAG
dag = DAG(
    'data_cleaning_dag',
    description='A DAG for cleaning and loading data',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2024, 1, 1),  # Adjust start date as needed
    catchup=False  # Do not backfill missed DAG runs
)

# defining tasks

clean_hr_data = PythonOperator(
    task_id = 'Clean_HR_Data',
    python_callable = etl.clean_hr_data,
    dag = dag
)

clean_satisfaction_data = PythonOperator(
    task_id = 'Clean_Survey_Data',
    python_callable = etl.clean_satisfaction_data,
    dag = dag
)

clean_attendance_data = PythonOperator(
    task_id = 'Clean_Attendance_Data',
    python_callable = etl.clean_attendance_data,
    dag = dag
)

join_data = PythonOperator(
    task_id = 'Transform_Data',
    python_callable = etl.transform_data,
    dag = dag
)

# task dependencies

clean_hr_data >> clean_satisfaction_data >> clean_attendance_data >> join_data