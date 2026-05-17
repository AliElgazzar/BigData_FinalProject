import json
import logging
import os
import random
import time

import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wikimedia-recent-change")
WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=100
            )
            logging.info("Connected to Kafka.")
            return producer
        except Exception as e:
            logging.error(f"Kafka not ready: {e}")
            time.sleep(5)


def send_message(producer, message):
    producer.send(KAFKA_TOPIC, message)
    producer.flush()
    logging.info(f"Sent event to Kafka: {message}")


def fallback_events(producer):
    servers = [
        ("en.wikipedia.org", "enwiki"),
        ("ar.wikipedia.org", "arwiki"),
        ("fr.wikipedia.org", "frwiki"),
        ("de.wikipedia.org", "dewiki"),
        ("commons.wikimedia.org", "commonswiki"),
        ("www.wikidata.org", "wikidatawiki")
    ]

    while True:
        server_name, wiki = random.choice(servers)
        message = {
            "id": int(time.time() * 1000),
            "type": random.choice(["edit", "new", "log"]),
            "title": random.choice(["Apache Kafka", "Apache Spark", "Hive", "Big Data", "Wikimedia"]),
            "namespace": 0,
            "editor_user": random.choice(["student_user", "demo_editor", "wiki_bot", "anonymous"]),
            "bot": random.choice([False, False, False, True]),
            "server_name": server_name,
            "wiki": wiki,
            "timestamp": int(time.time()),
            "comment": "generated event"
        }
        send_message(producer, message)
        time.sleep(1)


def main():
    producer = create_producer()

    headers = {
        "User-Agent": "CS523-BigData-FinalProject/1.0 (student project; contact: alielgazzar559@gmail.com)",
        "Accept": "text/event-stream"
    }

    try:
        logging.info(f"Connecting to Wikimedia stream {WIKIMEDIA_STREAM_URL}")
        response = requests.get(WIKIMEDIA_STREAM_URL, headers=headers, stream=True, timeout=20)
        response.raise_for_status()

        for line in response.iter_lines():
            if not line:
                continue

            decoded = line.decode("utf-8", errors="ignore")
            if not decoded.startswith("data: "):
                continue

            raw = json.loads(decoded[6:])
            message = {
                "id": raw.get("id"),
                "type": raw.get("type"),
                "title": raw.get("title"),
                "namespace": raw.get("namespace"),
                "editor_user": raw.get("user"),
                "bot": bool(raw.get("bot", False)),
                "server_name": raw.get("server_name"),
                "wiki": raw.get("wiki"),
                "timestamp": raw.get("timestamp"),
                "comment": raw.get("comment")
            }

            if message["server_name"] and message["timestamp"]:
                send_message(producer, message)

    except Exception as e:
        logging.error(f"Wikimedia stream unavailable: {e}")
        fallback_events(producer)


if __name__ == "__main__":
    main()
