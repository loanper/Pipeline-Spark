#!/bin/bash

echo "Démarrage du pipeline US Accidents..."

./download_data.sh
if [ $? -ne 0 ]; then exit 1; fi

docker compose down
docker compose up -d

echo "Attente du démarrage du cluster Spark (10s)..."
sleep 10

# 3. Permission to user
sudo chmod -R 777 src/

# 4. Exécution du Job Spark
echo "Lancement du traitement Spark..."
docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/job.py

echo "Terminé ! Vérifiez le dossier src/output_weather_analysis pour les résultats."