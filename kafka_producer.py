from kafka import KafkaProducer
import json
import time
import random
import pandas as pd

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Nom du topic Kafka
TOPIC = 'movies_ratings'

# Charger les données existantes pour avoir des IDs valides
ratings_df = pd.read_csv('rating.csv')
valid_user_ids = ratings_df['userId'].unique()
valid_movie_ids = ratings_df['movieId'].unique()

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
            # Générer et envoyer une nouvelle note
            msg = generate_rating()
            producer.send(TOPIC, msg)
            print(f"Message envoyé: {msg}")

            # Attendre avant d'envoyer le prochain message
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nArrêt de l'envoi des messages")
        producer.close()

if __name__ == "__main__":
    print(f"Démarrage du producteur Kafka sur le topic '{TOPIC}'...")
    send_ratings()