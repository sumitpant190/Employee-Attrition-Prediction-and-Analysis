import streamlit as st
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def initialize_spark():
    # Initialize PySpark session
    spark = SparkSession.builder \
        .appName("ModelLoader") \
        .getOrCreate()

    # Load the pipeline model
    pipeline_path = '/mnt/f/attrition_etl/eda/pipeline'
    pipeline_model = PipelineModel.load(pipeline_path)

    # Load the RandomForestClassificationModel from the specified path
    model_path = '/mnt/f/attrition_etl/eda/model/bestModel/'
    model = RandomForestClassificationModel.load(model_path)

    return spark, pipeline_model, model

def get_input_data():
    # User inputs
    age = st.number_input("Age", min_value=18, max_value=60, value=36)
    business_travel = st.selectbox("Business Travel", ["Travel_Rarely", "Travel_Frequently", "Non-Travel"])
    daily_rate = st.number_input("Daily Rate", min_value=102, max_value=1499, value=802)
    department = st.selectbox("Department", ["Research & Development", "Sales", "Human Resources"])
    distance_from_home = st.number_input("Distance From Home", min_value=1, max_value=29, value=9)
    education_field = st.selectbox("Education Field", ["Life Sciences", "Medical", "Marketing", "Technical Degree", "Human Resources", "Other"])
    environment_satisfaction = st.slider("Environment Satisfaction", min_value=1, max_value=4, value=3)
    job_involvement = st.slider("Job Involvement", min_value=1, max_value=4, value=3)
    job_level = st.slider("Job Level", min_value=1, max_value=5, value=2)
    job_role = st.selectbox("Job Role", ["Sales Executive", "Research Scientist", "Laboratory Technician", "Manufacturing Director", "Healthcare Representative", "Manager", "Sales Representative", "Research Director", "Human Resources"])
    job_satisfaction = st.slider("Job Satisfaction", min_value=1, max_value=4, value=3)
    marital_status = st.selectbox("Marital Status", ["Married", "Single", "Divorced"])
    monthly_income = st.number_input("Monthly Income", min_value=1009, max_value=19999, value=6502)
    over_time = st.selectbox("Over Time", ["Yes", "No"])
    stock_option_level = st.slider("Stock Option Level", min_value=0, max_value=3, value=0)
    total_working_years = st.number_input("Total Working Years", min_value=0, max_value=40, value=11)
    training_times_last_year = st.slider("Training Times Last Year", min_value=0, max_value=6, value=3)
    work_life_balance = st.slider("Work Life Balance", min_value=1, max_value=4, value=3)
    years_at_company = st.number_input("Years At Company", min_value=0, max_value=40, value=7)
    years_in_current_role = st.number_input("Years In Current Role", min_value=0, max_value=18, value=4)
    years_with_curr_manager = st.number_input("Years With Current Manager", min_value=0, max_value=17, value=4)

    # Create DataFrame from input
    input_data = [(age, business_travel, daily_rate, department, distance_from_home, education_field, environment_satisfaction, job_involvement, job_level, job_role, job_satisfaction, marital_status, monthly_income, over_time, stock_option_level, total_working_years, training_times_last_year, work_life_balance, years_at_company, years_in_current_role, years_with_curr_manager)]
    
    return input_data

def make_prediction(input_data, spark, pipeline_model, model):
    schema = StructType([
        StructField("Age", IntegerType(), True),
        StructField("BusinessTravel", StringType(), True),
        StructField("DailyRate", IntegerType(), True),
        StructField("Department", StringType(), True),
        StructField("DistanceFromHome", IntegerType(), True),
        StructField("EducationField", StringType(), True),
        StructField("EnvironmentSatisfaction", IntegerType(), True),
        StructField("JobInvolvement", IntegerType(), True),
        StructField("JobLevel", IntegerType(), True),
        StructField("JobRole", StringType(), True),
        StructField("JobSatisfaction", IntegerType(), True),
        StructField("MaritalStatus", StringType(), True),
        StructField("MonthlyIncome", IntegerType(), True),
        StructField("OverTime", StringType(), True),
        StructField("StockOptionLevel", IntegerType(), True),
        StructField("TotalWorkingYears", IntegerType(), True),
        StructField("TrainingTimesLastYear", IntegerType(), True),
        StructField("WorkLifeBalance", IntegerType(), True),
        StructField("YearsAtCompany", IntegerType(), True),
        StructField("YearsInCurrentRole", IntegerType(), True),
        StructField("YearsWithCurrManager", IntegerType(), True)
    ])

    input_df = spark.createDataFrame(input_data, schema=schema)

    # Transform the data using the loaded pipeline
    transformed_data = pipeline_model.transform(input_df)

    # Select the features column for prediction
    transformed_data = transformed_data.select('scaled_features')

    # Make prediction
    predictions = model.transform(transformed_data)

    # Extract prediction
    prediction = predictions.select('prediction').collect()[0]['prediction']

    return prediction

def main():
    st.title("Employee Attrition Predictor")
    spark, pipeline_model, model = initialize_spark()
    input_data = get_input_data()

    if st.button("Predict"):
        prediction = make_prediction(input_data, spark, pipeline_model, model)
        attrition_result = 'Yes' if prediction == 1.0 else 'No'
        st.subheader("Prediction Result")
        st.info(f"Predicted Attrition: {attrition_result}")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
