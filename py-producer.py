'''
Python Kafka Producer Example
'''

from kafka import KafkaProducer
import json
from time import sleep

import os
import json
import requests
from datetime import datetime, timezone

API_URL = "https://opensky-network.org/api/states/all"
OUT_DIR = "data/opensky_raw"

def fetch_and_save():
    os.makedirs(OUT_DIR, exist_ok=True)
    r = requests.get(API_URL, timeout=30)
    r.raise_for_status()
    payload = r.json()
    record = {
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "api_ts": payload.get("time"),
        "states": payload.get("states")
    }
    filename = f"opensky_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f)

def get_messages_from_api():
    r = requests.get(API_URL, timeout=30)
    r.raise_for_status()
    payload = r.json()
    record = {
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "api_ts": payload.get("time"),
        "states": payload.get("states")
    }
    return record

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer,
    max_request_size=10485760,
    batch_size=16384,  # 16k
    linger_ms=50
)

def produce_messages(topic, messages):
    for message in messages:
        producer.send(topic, value=message)
        print(f"Produced messages messgage to topic {topic}")
        #sleep(1)  # Simulate some delay between messages
    producer.flush()  # Ensure all messages are sent
    
if __name__ == "__main__":
    topic = 'myTopic' # Change to your topic name
    # loop over the API and produce messages and call every second
    while True:
        messages = [get_messages_from_api()]
        # messages is too big to be sent in one message, so we need to split it into smaller messages
        # we can split the states into smaller messages and send them separately
        states = messages[0]['states']
        batch_size = 100  # Number of states per message
        messages_to_send = []
        for i in range(0, len(states), batch_size):
            batch = states[i:i + batch_size]
            message = {
                "ingest_ts": messages[0]['ingest_ts'],
                "api_ts": messages[0]['api_ts'],
                "states": batch
            }
            messages_to_send.append(message)
            print(f"Prepared message with {len(batch)} states to send.")
        produce_messages(topic, messages_to_send)
        print("All messages produced.")
        exit()
        sleep(1)  # Wait for 1 second before fetching new messages