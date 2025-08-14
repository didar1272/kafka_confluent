from confluent_kafka import Consumer, KafkaException
import json


KAFKA_URL = 'localhost:9092'
TOPIC = 'weather_data'

conf = {
    "bootstrap.servers": KAFKA_URL,
    "auto.offset.reset": "earliest",
    "group.id": 'weather-data-consumer-group'
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

print("waiting for message")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print('No message!')
            continue
        if msg.error():
            raise KafkaException(msg.error())
        
        data = json.loads(msg.value().decode('utf-8'))
        print("Received data: ", data)

except KeyboardInterrupt:
    print("Stopped by user!")
finally:
    consumer.close()
