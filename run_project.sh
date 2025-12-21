#!/bin/bash

echo "Start of Pipeline US Accident"

docker compose down
docker compose up -d

echo "Wait of spark cluster being launch (15s)..."
sleep 15

sudo chmod -R 777 src/ 

echo "Launch of Spark..."
docker exec spark-master /opt/spark/bin/spark-submit \
  --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2/cache" \
  --conf "spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2/cache" \
  --conf "spark.jars.ivy=/tmp/.ivy2/jars" \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/work-dir/job.py
#installation of S3 drivers and redirection of cache directory due to an error with writting permissions inside of spark
