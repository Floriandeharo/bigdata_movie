from kafka import KafkaProducer
from pyspark.sql import SparkSession
import json
import time
import random

# Initialiser SparkSession
spark = SparkSession.builder \
    .appName("KafkaProducerSpark") \
    .getOrCreate()

# Kafka Producer config
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10)
)

# Nom du topic Kafka
TOPIC = 'movies_ratings'

# Charger les données depuis HDFS avec Spark
ratings_df = spark.read.csv("hdfs://namenode:9000/datasets/rating.csv", header=True, inferSchema=True)

# Extraire les IDs valides pour la simulation
valid_user_ids = [row['userId'] for row in ratings_df.select("userId").distinct().collect()]
valid_movie_ids = [row['movieId'] for row in ratings_df.select("movieId").distinct().collect()]

def generate_rating():
    """Génère une nouvelle note aléatoire"""
    return {
        'userId': int(random.choice(valid_user_ids)),
        'movieId': int(random.choice(valid_movie_ids)),
        'rating': round(random.uniform(0.5, 5.0), 1),
        'timestamp': int(time.time())
    }

def send_ratings(interval=2):
    """Envoie des notes en continu avec un intervalle donné"""
    try:
        while True:
            msg = generate_rating()
            producer.send(TOPIC, msg)
            print(f"Message envoyé: {msg}")
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nArrêt de l'envoi des messages")
        producer.close()

if __name__ == "__main__":
    print(f"Démarrage du producteur Kafka sur le topic '{TOPIC}'...")
    send_ratings()
