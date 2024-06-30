import atexit
from pyspark.sql import SparkSession
import boto3
import os 

# AWS S3 configurations
#aws_access_key_id = ' ' your access key id
#aws_secret_access_key = ' ' your secret access key
aws_region = ' '
bucket_name = 'attrition-etl-project'
survey_data_prefix = ''
satisfaction_file_key = 'Employee Satisfaction Index.csv'
attendance_file_key = 'Absenteeism_at_work.csv'
local_staging_dir = '/mnt/f/attrition_etl/staging'

# Provide the correct path to the necessary JAR file
jdbc_driver_path = r"/mnt/f/postgresql-42.7.3.jar"

# defining Postgresql connection properties
postgres_url = 'jdbc:postgresql://localhost:5432/attrition_db'
properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
        }
    
# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)


# Initialize a single Spark Session at module level
spark = SparkSession.builder \
    .appName("Data Preprocessing") \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.executor.extraClassPath", jdbc_driver_path) \
    .getOrCreate()

def extract_hr_data():
    #extracting hr_data from local storage
    attrition_hr_data_path = "/mnt/f/attrition_etl/WA_Fn-UseC_-HR-Employee-Attrition.csv"
    attrition_df = spark.read.csv(attrition_hr_data_path,header= True,inferSchema= True)

    # saving hr data to postgresql staging area
    
    attrition_df.write.jdbc(url=postgres_url,table='attrition_table',mode="overwrite",properties=properties)

def extract_satisfaction_data():
    # extract data from s3
    """Extract survey data from S3 and store it in PostgreSQL."""
    if not os.path.exists(local_staging_dir):
        os.makedirs(local_staging_dir)

    local_file_path = os.path.join(local_staging_dir, os.path.basename(satisfaction_file_key))
    s3_client.download_file(bucket_name, satisfaction_file_key, local_file_path)
    print(f'Downloaded {satisfaction_file_key} to {local_file_path}')

    # Read the CSV file into a Spark Dataframe
    satisfaction_df = spark.read.csv(local_file_path, header= True, inferSchema= True)

    satisfaction_df.write.jdbc(url=postgres_url, table='satisfaction_table', mode ='append',properties=properties)
    print(f'Saved data from {local_file_path} to PostgreSQL')

def extract_attendance_data():
    # extract data from s3
    """Extract survey data from S3 and store it in PostgreSQL."""
    if not os.path.exists(local_staging_dir):
        os.makedirs(local_staging_dir)

    local_file_path = os.path.join(local_staging_dir, os.path.basename(attendance_file_key))
    s3_client.download_file(bucket_name, attendance_file_key, local_file_path)
    print(f'Downloaded {attendance_file_key} to {local_file_path}')

    # Read the CSV file into a Spark Dataframe
    satisfaction_df = spark.read.csv(local_file_path, header= True, inferSchema= True, sep=';')

    satisfaction_df.write.jdbc(url=postgres_url, table='attendance_table', mode ='append',properties=properties)
    print(f'Saved data from {local_file_path} to PostgreSQL')



def cleanup_spark_session():
    if spark is not None:
        spark.stop()\
        
atexit.register(cleanup_spark_session)

    


