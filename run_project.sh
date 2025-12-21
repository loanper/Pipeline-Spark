#!/bin/bash

echo "Démarrage du pipeline US Accidents..."


docker compose down
docker compose up -d

echo "Attente du démarrage du cluster Spark (10s)..."
sleep 10

# 3. Permission to user
sudo chmod -R 777 src/

# 4. Exécution du Job Spark avec redirection totale d'Ivy
echo "📊 Lancement du traitement Spark (S3 & Permissions)..."
docker exec spark-master /opt/spark/bin/spark-submit \
  --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2/cache" \
  --conf "spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2/cache" \
  --conf "spark.jars.ivy=/tmp/.ivy2/jars" \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark/work-dir/job.py

echo "Terminé ! Vérifiez le dossier src/output_weather_analysis pour les résultats."