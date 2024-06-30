import atexit
from pyspark.sql import SparkSession

# Provide the correct path to the necessary JAR file
jdbc_driver_path = "/mnt/f/postgresql-42.7.3.jar"

# Defining PostgreSQL connection properties
attrition_postgres_url = 'jdbc:postgresql://localhost:5432/attrition_db'
cleaned_data_postgres_url = 'jdbc:postgresql://localhost:5432/cleaned_data'
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Cleaning and Transformation") \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.executor.extraClassPath", jdbc_driver_path) \
    .getOrCreate()

# Define ETL functions

def clean_hr_data():
    attrition_df = spark.read.jdbc(url=attrition_postgres_url, table="attrition_table", properties=properties)
    
    # Cleaning null values 
    attrition_df = attrition_df.dropna()

    # Removing duplicates
    attrition_df_cleaned = attrition_df.dropDuplicates()

    # Writing to staging area
    attrition_df_cleaned.write.jdbc(url=cleaned_data_postgres_url, table='attrition_cleaned', mode="overwrite", properties=properties)


def clean_satisfaction_data():
    satisfaction_df = spark.read.jdbc(url=attrition_postgres_url, table="satisfaction_table", properties=properties)

     # Cleaning null values 
    satisfaction_df = satisfaction_df.dropna()

    # Removing duplicates
    satisfaction_df_cleaned = satisfaction_df.dropDuplicates()

    satisfaction_df_cleaned.write.jdbc(url=cleaned_data_postgres_url, table='satisfaction_cleaned', mode="overwrite", properties=properties)


def clean_attendance_data():
    attendance_df = spark.read.jdbc(url=attrition_postgres_url, table="attendance_table", properties=properties)

     # Cleaning null values 
    attendance_df = attendance_df.dropna()

    # Removing duplicates
    attendance_df_cleaned = attendance_df.dropDuplicates()

    attendance_df_cleaned.write.jdbc(url=cleaned_data_postgres_url, table='attendance_cleaned', mode="overwrite", properties=properties)

def transform_data():
    #joining attrition table and attendance table 
    # fetching tables to integrate
    attrition_df = spark.read.jdbc(url=cleaned_data_postgres_url, table="attrition_cleaned", properties=properties)
    attendance_df = spark.read.jdbc(url=cleaned_data_postgres_url, table="attendance_cleaned", properties=properties)

    conflict_columns = ['Age', 'Education', 'Distance from Residence to Work']

    # Drop the conflicting columns from attendance_df
    attendance_df = attendance_df.drop(*conflict_columns)

    # Specify the columns to join on
    join_expr = attrition_df["EmployeeNumber"] == attendance_df["ID"]

    # Perform the left join
    joined_df = attrition_df.join(attendance_df, join_expr, "left")

    joinded_df = joined_df.dropna()

    joined_df.write.jdbc(url = cleaned_data_postgres_url, table = 'attrition_attendance_cleaned',mode="overwrite", properties=properties )


def cleanup_spark_session():
    if spark is not None:
        spark.stop()

# Register Spark session cleanup on script exit
atexit.register(cleanup_spark_session)
