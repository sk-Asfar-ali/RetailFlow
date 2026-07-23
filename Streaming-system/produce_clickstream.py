"""
produce_clickstream.py
-----------------------
Continuously generates clickstream events and publishes them to either
Azure Event Hubs or a local Kafka cluster, chosen via STREAM_BACKEND
in .env. This is the streaming counterpart to simulate_traffic.py in
the source-system folder.

Usage:
    python produce_clickstream.py --rate 5   # ~5 events/sec

Optionally pulls real customer_ids from the Postgres source system so
events reference actual customers (set PG* env vars to enable; falls
back to anonymous-only events if unavailable).
"""

import argparse
import os
import time

from dotenv import load_dotenv

from events import SessionPool, generate_event, event_to_json

load_dotenv()

BACKEND = os.getenv("STREAM_BACKEND", "kafka").lower()


def fetch_customer_ids():
    """Best-effort pull of real customer_ids from the source Postgres DB."""
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            port=os.getenv("PGPORT", "5432"),
            dbname=os.getenv("PGDATABASE", "ecommerce_source"),
            user=os.getenv("PGUSER", "source_admin"),
            password=os.getenv("PGPASSWORD", "source_pass"),
            connect_timeout=3,
        )
        cur = conn.cursor()
        cur.execute("SELECT customer_id FROM retail.customers LIMIT 1000")
        ids = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        print(f"Loaded {len(ids)} real customer_ids from source Postgres DB.")
        return ids
    except Exception as e:
        print(f"Could not load customer_ids from Postgres ({e}). Using anonymous-only sessions.")
        return []


class ConsoleSender:
    def __init__(self):
        print("Connected to Console/Stdout mode (printing JSON clickstream events)")

    def send(self, event_json: str):
        print(f"[EVENT EMITTED]: {event_json}")

    def close(self):
        pass


class KafkaSender:
    def __init__(self):
        from kafka import KafkaProducer

        servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "clickstream-events")
        self.producer = KafkaProducer(
            bootstrap_servers=servers.split(","),
            value_serializer=lambda v: v.encode("utf-8"),
        )
        print(f"Connected to Kafka at {servers}, topic '{self.topic}'")

    def send(self, event_json: str):
        self.producer.send(self.topic, value=event_json)

    def close(self):
        self.producer.flush()
        self.producer.close()


class EventHubSender:
    def __init__(self):
        from azure.eventhub import EventHubProducerClient, EventData

        self.EventData = EventData
        conn_str = os.getenv("EVENTHUB_CONNECTION_STR")
        eventhub_name = os.getenv("EVENTHUB_NAME", "clickstream-events")
        if not conn_str or "<your-namespace>" in conn_str:
            raise RuntimeError(
                "EVENTHUB_CONNECTION_STR is not set. Update .env with your "
                "Event Hubs namespace connection string."
            )
        self.client = EventHubProducerClient.from_connection_string(
            conn_str=conn_str, eventhub_name=eventhub_name
        )
        print(f"Connected to Event Hub '{eventhub_name}'")

    def send(self, event_json: str):
        batch = self.client.create_batch()
        batch.add(self.EventData(event_json))
        self.client.send_batch(batch)

    def close(self):
        self.client.close()


def get_sender(backend_override=None):
    backend = (backend_override or BACKEND).lower()
    if backend == "eventhub":
        return EventHubSender()
    elif backend == "kafka":
        return KafkaSender()
    elif backend == "console":
        return ConsoleSender()
    else:
        raise ValueError(f"Unknown STREAM_BACKEND '{backend}', expected 'eventhub', 'kafka', or 'console'")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    parser.add_argument("--pool-size", type=int, default=200, help="concurrent simulated sessions")
    parser.add_argument("--console", action="store_true", help="Print clickstream events to stdout console")
    args = parser.parse_args()

    customer_ids = fetch_customer_ids()
    session_pool = SessionPool(customer_ids=customer_ids, pool_size=args.pool_size)
    
    backend_choice = "console" if args.console else None
    sender = get_sender(backend_choice)

    backend_name = "console" if args.console else BACKEND
    delay = 1.0 / args.rate
    sent = 0
    print(f"Streaming events via '{backend_name}' backend at ~{args.rate}/sec. Ctrl+C to stop.")


    try:
        while True:
            event = generate_event(session_pool)
            sender.send(event_to_json(event))
            sent += 1
            if sent % 50 == 0:
                print(f"Sent {sent} events...")
            time.sleep(delay)
    except KeyboardInterrupt:
        print(f"\nStopping. Total events sent: {sent}")
    finally:
        sender.close()


if __name__ == "__main__":
    main()
