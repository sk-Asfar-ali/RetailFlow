# Streaming System — Clickstream Event Producer

Simulates live user-activity (clickstream) events — page views, product
views, searches, cart actions, checkout starts — and publishes them
continuously to a message broker. This is the streaming half of the
project, pairing with the batch OLTP data in `source-system/`.

Supports two backends, switchable via `.env`:
- **`kafka`** — local Kafka via Docker, free, no cloud account needed. Good for building/testing the pipeline first.
- **`eventhub`** — Azure Event Hubs, Kafka-protocol compatible, integrates directly with Databricks Structured Streaming. Use this for the "real" cloud version.

## Option A: Local Kafka (recommended to start)

```bash
docker compose -f docker-compose.kafka.yml up -d
```

This starts Kafka + Zookeeper + Kafka UI (browse topics at `localhost:8080`).

In `.env`, set:
```
STREAM_BACKEND=kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=clickstream-events
```

## Option B: Azure Event Hubs

1. In the Azure Portal: create an **Event Hubs Namespace** (Basic tier is fine and cheap), then an **Event Hub** inside it named e.g. `clickstream-events`.
2. Under the namespace's **Shared access policies**, grab a connection string with Send permission.
3. In `.env`, set:
```
STREAM_BACKEND=eventhub
EVENTHUB_CONNECTION_STR=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...
EVENTHUB_NAME=clickstream-events
```

## Install deps

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run the producer

```bash
python produce_clickstream.py --rate 5 --pool-size 200
```

- `--rate`: events per second (try 5–20 for a realistic feel)
- `--pool-size`: number of concurrent simulated user sessions

If the Postgres source system (`../source-system`) is running and seeded,
the producer will automatically pull real `customer_id`s so ~60% of
clickstream events are tied to actual customers — letting you join
clickstream (Silver) with orders (Silver) downstream in Gold. If Postgres
isn't reachable, it falls back to anonymous-only sessions with no errors.

## Event schema

```json
{
  "event_id": "uuid",
  "event_type": "page_view | product_view | search | add_to_cart | remove_from_cart | checkout_start",
  "event_time": "ISO8601 UTC",
  "session_id": "uuid",
  "customer_id": "int or null (null = anonymous)",
  "device": "mobile | desktop | tablet",
  "page_url": "string",
  "referrer": "string or null",
  "user_agent": "string",
  "ip_address": "string",
  "product_id": "int (only for product_view/add_to_cart/remove_from_cart)",
  "quantity": "int (only for add_to_cart/remove_from_cart)",
  "search_term": "string (only for search)",
  "cart_value": "float (only for checkout_start)"
}
```

## Next steps

- Databricks Structured Streaming reads from this topic/Event Hub into a
  Bronze Delta table (`bronze_clickstream_events`), using the Kafka
  connector (Event Hubs' Kafka-compatible endpoint works with the same
  `spark.readStream.format("kafka")` API as local Kafka — same code either way).
- Pair with the Bronze orders/customers tables from the batch source
  system, then build Silver/Gold with dbt.
