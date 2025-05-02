from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
import pandas as pd
from datetime import datetime

# Configuration du consommateur Kafka
consumer = KafkaConsumer(
    'movies_ratings',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest'
)

# Initialiser Spark
spark = SparkSession.builder \
    .appName("MovieRecommendationStreaming") \
    .getOrCreate()

# Charger le modèle ALS
model = ALSModel.load("hdfs://namenode:9000/models/als_model")


# Charger les informations des films
movies_df = spark.read.csv("hdfs://namenode:9000/datasets/movie.csv", header=True, inferSchema=True)


def process_message(msg):
    """Traite un message reçu de Kafka"""
    try:
        # Extraire les données du message
        data = msg.value
        print(f"\nNouvelle note reçue: {data}")

        # Créer un DataFrame Spark avec la nouvelle note
        new_rating = spark.createDataFrame([data])

        # Générer des recommandations
        predictions = model.transform(new_rating)

        # Joindre avec les informations des films
        recommendations = predictions.join(movies_df, 'movieId')

        # Afficher les recommandations
        print("\nRecommandations générées:")
        recommendations.show(5, truncate=False)

        # Sauvegarder les recommandations
        save_recommendations(recommendations)

    except Exception as e:
        print(f"Erreur lors du traitement du message: {e}")

def save_recommendations(recommendations):
    """Sauvegarde les recommandations dans un fichier CSV"""
    # Convertir en pandas DataFrame
    recommendations_pd = recommendations.toPandas()

    # Ajouter un timestamp
    recommendations_pd['generated_at'] = datetime.now()

    # Sauvegarder dans un fichier
    filename = 'streaming_recommendations.csv'
    recommendations_pd.to_csv(
        filename,
        mode='a',
        header=False,
        index=False
    )

def start_consuming():
    """Démarre la consommation des messages"""
    print("Démarrage du consommateur Kafka...")
    try:
        for msg in consumer:
            process_message(msg)

    except KeyboardInterrupt:
        print("\nArrêt du consommateur")
        consumer.close()

if __name__ == "__main__":
    start_consuming()