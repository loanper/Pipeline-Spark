#!/bin/bash

DATA_DIR="./src"
FILE_NAME="dataset.csv"
ZIP_NAME="archive.zip"
KAGGLE_URL="https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents/download?datasetVersionNumber=12"

echo "=== Vérification des données ==="

if [ -f "$DATA_DIR/$FILE_NAME" ]; then
    echo "[OK] Le dataset est déjà présent dans $DATA_DIR."
else
    echo "[!] Dataset introuvable dans $DATA_DIR."
    echo "En raison des restrictions de Kaggle, veuillez télécharger manuellement le dataset ici :"
    echo "$KAGGLE_URL"
    echo "Puis placez le fichier CSV dans le dossier 'src' et renommez-le en 'dataset.csv'."
    exit 1
fi