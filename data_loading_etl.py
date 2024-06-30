from pyspark.sql import SparkSession
import logging

def etl_postgres_to_postgres():
    # Initialize Spark Session
    # Provide the correct path to the necessary JAR file
    jdbc_driver_path = "/mnt/f/postgresql-42.7.3.jar"
    
    # Defining PostgreSQL connection properties
    cleaned_data_postgres_url = 'jdbc:postgresql://localhost:5432/cleaned_data'
    warehouse_data_postgres_url = 'jdbc:postgresql://localhost:5432/warehouse_data'   
    properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Initialize Spark session
    logging.info("Initializing Spark Session")
    spark = SparkSession.builder \
        .appName("Data Cleaning and Transformation") \
        .config("spark.driver.extraClassPath", jdbc_driver_path) \
        .config("spark.executor.extraClassPath", jdbc_driver_path) \
        .getOrCreate()

    # Read data from PostgreSQL into Spark DataFrames
    logging.info("Reading data from PostgreSQL")
    
    attrition_df_cleaned = spark.read.jdbc(url=cleaned_data_postgres_url, table='attrition_cleaned', properties=properties)
    attendance_df_cleaned = spark.read.jdbc(url=cleaned_data_postgres_url, table='attendance_cleaned', properties=properties)
    attrition_attendance_cleaned = spark.read.jdbc(url=cleaned_data_postgres_url, table='attrition_attendance_cleaned', properties=properties)


    attrition_attendance_cleaned = attrition_attendance_cleaned.dropna()

    attrition_attendance_cleaned.write.jdbc(url= warehouse_data_postgres_url ,table='attrition_attendance_cleaned',mode="overwrite",properties=properties)
   

    spark.stop()

# Run the ETL process
etl_postgres_to_postgres()
