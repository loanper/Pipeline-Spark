"""
Big Data Processing Pipeline: US Accidents Analysis
This script leverages PySpark to process large-scale data stored in a MinIO Data Lake.
Authors: Loan PERRARD & Quentin HEITZ
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import os

# Initialize Spark Session with S3A configuration for MinIO compatibility
# Credentials are retrieved from environment variables for security

spark = SparkSession.builder.appName("US_Accidents_S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


try:
    # Load dataset from MinIO bucket using S3A protocol
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://accidents-data/dataset.csv")
    df.createOrReplaceTempView("accidents")

# ANALYSIS 1: Identifying the top 10 states with the highest accident frequency
    
    print("\n>>> Analysis 1 : Top 10 of States with the most accidents")
    query_states = """
        SELECT 
            State, 
            COUNT(ID) as total_accidents
        FROM accidents 
        GROUP BY State 
        ORDER BY total_accidents DESC 
        LIMIT 10"""
    
    result_states = spark.sql(query_states)
    result_states.show()

# ANALYSIS 2: Evaluating how weather conditions impact accident severity
# Filter applied: only conditions with >1000 occurrences to ensure statistical relevance
    
    print("\n>>> Analysis 2 : Average severity by weather conditions")
    query_weather = """
        SELECT 
            Weather_Condition, 
            COUNT(ID) as count,
            ROUND(AVG(Severity), 2) as avg_severity
        FROM accidents 
        WHERE Weather_Condition IS NOT NULL
        GROUP BY Weather_Condition 
        HAVING count > 1000  -- We filter to minimum frequency so we don't get special cases
        ORDER BY avg_severity DESC
        LIMIT 20
    """
    result_weather = spark.sql(query_weather)
    result_weather.show(truncate=False) # truncate=False to see long words

# Exporting results to CSV format for downstream visualization
# Using 'overwrite' mode to allow script re-runs without folder conflicts
    
    result_states.write.mode("overwrite").csv("/opt/spark/work-dir/output_states_analysis", header=True)
    result_weather.write.mode("overwrite").csv("/opt/spark/work-dir/output_weather_analysis", header=True)
    print("Succeed ! You can find .csv files in src/ to see the results !")
    
except Exception as e:
    print("ERROR")
    print(e)


spark.stop()
