from kafka import KafkaProducer
import json
import requests
import time

# Configuration OpenWeatherMap
API_KEY = "5118f09baba8161f6d77072da6d6cfb4"  
CITIES = ['Paris', 'London', 'Tokyo', 'Bangkok', 'Berlin']
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

# Configuration Kafka
KAFKA_BROKER = "localhost:9092"  # Mettez l'adresse de votre broker Kafka
TOPIC = "projet-esme"

# Création du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_weather_data():
    """Récupère les données météo depuis OpenWeatherMap."""
    try:
        response = requests.get(URL)
        if response.status_code == 200:
            data = response.json()
            weather_info = {
                "city": data["name"],
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["description"],
                "timestamp": data["dt"]
            }
            return weather_info
        else:
            print(f"Erreur API: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Erreur de connexion: {e}")
        return None

# Boucle d'envoi des données toutes les 10 secondes
while True:
    weather_data = get_weather_data()
    if weather_data:
        producer.send(TOPIC, value=weather_data)
        print(f"Données envoyées: {weather_data}")
    time.sleep(10)