from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder \
    .appName("MonPremierJobSpark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


try:
    df = spark.read.option("header", "true").csv("/opt/spark/work-dir/dataset.csv")
    df.show(n=1)

    df.createOrReplaceTempView("table")

    query = 'SELECT ID FROM table'

    result = spark.sql(query)
    print(">>> Résultat final :")

    result.show()
    

except Exception as e:
    print("!!! UNE ERREUR EST SURVENUE !!!")
    print(e)


spark.stop()