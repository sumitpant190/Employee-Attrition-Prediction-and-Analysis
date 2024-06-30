# retrain_model.py

from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def retrain_model():
    """
    Function to retrain a model using new data.

    This function assumes the paths to pipeline model and existing model
    are predefined and accessible.

    Returns:
    - None
    """
    # Paths to be adjusted as per your environment
    pipeline_path = '/mnt/f/attrition_etl/eda/pipeline'
    model_path = '/mnt/f/attrition_etl/eda/model/bestModel/'
    new_data_path = '/mnt/f/attrition_etl/new_data.csv'

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ModelRetrainer") \
        .getOrCreate()

    try:
        # Load pipeline model
        pipeline_model = PipelineModel.load(pipeline_path)

        # Load existing RandomForestClassificationModel
        model = RandomForestClassificationModel.load(model_path)

        # Load new data
        new_data = spark.read.csv(new_data_path, header=True, inferSchema=True)

        # Transform new data using pipeline model
        transformed_data = pipeline_model.transform(new_data)
        transformed_data = transformed_data.select('scaled_features','Attrition')

        # Retrain the model on new data
        model_updated = model.fit(transformed_data)

        # Save the updated model
        model_updated.write().overwrite().save(model_path)
        

    finally:
        # Stop Spark session
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    retrain_model()
