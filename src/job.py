from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("US_Accidents_S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


try:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://accidents-data/dataset.csv")
    df.createOrReplaceTempView("accidents")


    print("\n>>> Analyse 1 : Top 10 des États avec le plus d'accidents")
    query_states = """
        SELECT 
            State, 
            COUNT(ID) as total_accidents
        FROM accidents 
        GROUP BY State 
        ORDER BY total_accidents DESC 
        LIMIT 10"""
    
    spark.sql(query_states).show()
    
    # --- ANALYSE 2 : IMPACT DE LA MÉTÉO SUR LA SÉVÉRITÉ ---
    # On regarde la sévérité moyenne (1 à 4) selon la météo
    print("\n>>> Analyse 2 : Sévérité moyenne par condition météo (pour les conditions fréquentes)")
    query_weather = """
        SELECT 
            Weather_Condition, 
            COUNT(ID) as count,
            ROUND(AVG(Severity), 2) as avg_severity
        FROM accidents 
        WHERE Weather_Condition IS NOT NULL
        GROUP BY Weather_Condition 
        HAVING count > 1000  -- On filtre les cas rares pour garder des stats fiables
        ORDER BY avg_severity DESC
        LIMIT 20
    """
    result_weather = spark.sql(query_weather)
    result_weather.show(20, truncate=False) # truncate=False pour bien voir les noms longs

    print("\n>>> Sauvegarde des résultats...")
    # On sauvegarde le résultat de l'analyse météo dans un dossier
    result_weather.write.mode("overwrite").csv("/opt/spark/work-dir/output_weather_analysis", header=True)
    print(">>> Sauvegarde terminée dans /opt/spark/work-dir/output_weather_analysis")
    
except Exception as e:
    print("!!! UNE ERREUR EST SURVENUE !!!")
    print(e)


spark.stop()