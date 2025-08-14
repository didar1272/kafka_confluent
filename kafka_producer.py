from confluent_kafka import Producer
import requests
import json
import time

KAFKA_URL = 'localhost:9092'
TOPIC = 'weather_data'
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

kafka_producer = Producer({'bootstrap.servers': 'localhost:9092'})
print(kafka_producer)


def fetch_weather_data() -> dict | None:
    weather_data = requests.get(url=WEATHER_URL, params={
        "latitude": 23.777176,
        "longitude": 90.399452,
        "current": "temperature_2m"
    })
    weather_data = weather_data.json()
    print(weather_data)
    return weather_data

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery Failed : {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_messages():
    data = fetch_weather_data()
    for item in data:
        kafka_producer.poll(0)
        kafka_producer.produce(TOPIC,
            value = json.dumps(item), callback=delivery_report)
        time.sleep(0.2)

    kafka_producer.flush()
    
if __name__ == "__main__":        
    produce_messages()   

