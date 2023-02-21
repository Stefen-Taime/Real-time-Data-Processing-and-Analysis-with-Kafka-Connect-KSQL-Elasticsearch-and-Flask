from confluent_kafka import Producer
import json

def delivery_callback(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def read_json_file(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return data

def produce_to_topic(topic_name, data):
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    for row in data:
        # Convert keys to lowercase
        row = {k.lower(): v for k, v in row.items()}
        try:
            producer.produce(topic_name, json.dumps(row), callback=delivery_callback)
        except BufferError as e:
            print(f"Message queue is full ({len(producer)} messages awaiting delivery): {e}")
        producer.poll(0)

    producer.flush()

if __name__ == "__main__":
    data = read_json_file("path/scripts/us_cities.json")
    produce_to_topic("city_data", data)
