import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import time

# Configuration de la page
st.set_page_config(
    page_title="Movie Recommendations Dashboard",
    layout="wide"
)

# Titre
st.title("🎬 Système de Recommandation de Films en Temps Réel")

# Créer deux colonnes
col1, col2 = st.columns(2)

# Colonne de gauche : Dernières recommandations
with col1:
    st.subheader("📊 Dernières Recommandations")
    recommendations_table = st.empty()

    # Graphique des notes
    ratings_chart = st.empty()

# Colonne de droite : Statistiques
with col2:
    st.subheader("🎯 Statistiques en Direct")

    # Métriques
    metrics_container = st.container()
    total_count = metrics_container.empty()
    avg_rating = metrics_container.empty()

def update_dashboard():
    try:
        # Lire les dernières recommandations
        df = pd.read_csv('streaming_recommendations.csv')

        # Afficher les 5 dernières recommandations
        recommendations_table.dataframe(df.tail(5))

        # Créer un graphique des notes
        fig = px.line(
            df.tail(50),
            x='generated_at',
            y='rating',
            title='Évolution des Notes'
        )
        ratings_chart.plotly_chart(fig)

        # Mettre à jour les métriques
        total_count.metric(
            "Total Recommandations",
            len(df)
        )
        avg_rating.metric(
            "Note Moyenne",
            f"{df['rating'].mean():.2f}"
        )

    except Exception as e:
        st.error(f"Erreur lors de la mise à jour: {e}")

# Boucle principale
if __name__ == "__main__":
    while True:
        update_dashboard()
        time.sleep(1)