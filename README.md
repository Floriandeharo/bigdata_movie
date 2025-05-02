#  Projet Big Data — Système de Recommandation de Films avec MovieLens



##  Stack Technique

- Apache Spark (PySpark)
- Hadoop HDFS
- Apache Kafka + Kafka Python
- Jupyter Notebook
- Python 3.10
- Streamlit

---

##  Arborescence

```
notebooks/
├── ProjetBigData.ipynb         # Notebook d'entraînement du modèle
├── kafka_producer.py           # Producteur Kafka simulant les notes
├── kafka_consumer.py           # Consommateur Kafka (streaming Spark)
├── dashboard.py                # Dashboard Streamlit
├── rating.csv                  # Notes MovieLens
├── movie.csv                   # Films MovieLens
```

---

##  Lancement du projet

### 1. Construire l’image Docker

```bash
docker compose build
```

### 2. Lancer les services

```bash
docker compose up -d
```

---

##  Stockage des données dans HDFS

Depuis le container :

```bash
hdfs dfs -mkdir -p /datasets
hdfs dfs -put /notebooks/rating.csv /datasets/
hdfs dfs -put /notebooks/movie.csv /datasets/
```

---

##  Entraînement du modèle ALS

Dans le notebook `ProjetBigData.ipynb` :

- Charger les données depuis HDFS
- Entraîner le modèle `ALS`
- Sauvegarder le modèle :

```python
model.save("hdfs://namenode:9000/models/als_model")
```

---

## 📡 Traitement temps réel avec Kafka

### 1. Lancer le producteur

```bash
python3 /notebooks/kafka_producer.py
```

Cela enverra des messages simulés dans le topic `movies_ratings`.

### 2. Lancer le consommateur Spark

Dans un autre terminal :

```bash
python3 /notebooks/kafka_consumer.py
```

Ce script :
- Consomme le topic Kafka
- Applique le modèle ALS pour générer des prédictions
- Sauvegarde dans `streaming_recommendations.csv`

---

## Tableau de bord

Lancer Streamlit :

```bash
streamlit run /notebooks/dashboard.py --server.port=8501
```

Puis accéder à : [http://localhost:8501](http://localhost:8501)

---

