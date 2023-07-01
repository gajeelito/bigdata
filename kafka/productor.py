import requests
from confluent_kafka import Producer
from pymongo import MongoClient
import json
import time


d1 = {
    "bootstrap.servers":"pkc-03gz2.southamerica-west1.gcp.confluent.cloud:9092",
    "security.protocol":"SASL_SSL",
    "sasl.mechanisms":"PLAIN",
    "sasl.username":"JQXUW4CNJNF5J527",
    "sasl.password":"RLLtdvhEqFDc36b0kD6LOHvq6ku5xaTZZ8wWrHzPO4lERj9eNEEnQ4Folvw7FPT8"
}



producer = Producer(d1)

# URL de la API del clima
url = "https://api.openweathermap.org/data/2.5/weather?units=metric&lat=-33.499892&lon=-70.615829&appid=896407728aaff9977aa6af2cdff722fd"

# Conexión a la base de datos MongoDB
uri = "mongodb+srv://pablvalenzuela:lascumbres2312@cluster0.cceutla.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri)
db = client.am
users = db.users

while True:
    # Realizar una solicitud GET a la API
    response = requests.get(url)

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        # Obtener el JSON de la respuesta
        response_json = response.json()

        # Obtener el valor de la clave "main" dentro de "weather"
        weather_main = response_json["weather"][0]["main"]

        # Obtener el valor de la clave "temp" dentro de "main"
        temp = response_json["main"]["temp"]

        # Obtener el valor de la clave "country" dentro de "sys"
        country = response_json["sys"]["country"]

        # Obtener el valor de la clave "name" dentro de "sys"
        name = response_json["name"]

        # Crear un diccionario con los valores extraídos
        data = {
            "weather_main": weather_main,
            "temp": temp,
            "country": country,
            "name": name
        }

        # Convertir el diccionario a una cadena JSON
        json_str = json.dumps(data)

        # Codificar la cadena JSON en bytes
        message_bytes = json_str.encode()

        # Enviar los datos al topic usando el productor
        producer.produce("topic_1", key="key", value=message_bytes)
        producer.poll()
    else:
        print("Error al obtener los datos del clima")

    # Esperar 5 segundos antes de realizar la siguiente solicitud
    time.sleep(5)
