from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Import your ETL function
import data_extraction_etl as etl

# Define your DAG
dag = DAG(
    'data_extraction_dag',
    description='A DAG for extracting HR data',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2024, 1, 1),  # Adjust start date as needed
    catchup=False  # Do not backfill missed DAG runs
)

# Define tasks
extract_hr_data = PythonOperator(
    task_id='Extract_HR_Data',
    python_callable=etl.extract_hr_data,
    dag=dag
)

extract_survey_data = PythonOperator(
    task_id='Extract_Survey_Data',
    python_callable=etl.extract_satisfaction_data,
    dag=dag
)

extract_attendance_data = PythonOperator(
    task_id='Extract_Attendance_data',
    python_callable=etl.extract_attendance_data,
    dag=dag
)

extract_hr_data >> extract_survey_data >> extract_attendance_data

