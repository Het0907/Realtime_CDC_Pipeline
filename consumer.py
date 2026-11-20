"""
Simple CDC consumer: reads change events from Kafka topics produced by Debezium
and lands them as JSON files, simulating a 'bronze layer' in a lakehouse.

Install dependency first:
    pip install kafka-python

Run:
    python consumer.py
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError

parser = argparse.ArgumentParser(description="CDC Bronze Consumer")
parser.add_argument("--batch", action="store_true", help="Drain current messages from Kafka and exit")
parser.add_argument("--timeout", type=int, default=3000, help="Timeout in ms when --batch is used")
parser.add_argument("--group-id", default="bronze-loader", help="Kafka consumer group id")
parser.add_argument("--bootstrap-server", default="localhost:9093", help="Kafka bootstrap server")
parser.add_argument("--output-dir", default="bronze_layer", help="Bronze output directory")
parser.add_argument("--connect-retries", type=int, default=10, help="Kafka startup attempts")
parser.add_argument("--retry-delay", type=float, default=2.0, help="Seconds between Kafka startup attempts")
args, _ = parser.parse_known_args()

# Topics to consume - one per table Debezium is watching
TOPICS = [
    "dbserver1.inventory.customers",
    "dbserver1.inventory.orders",
    "dbserver1.inventory.products",
]

OUTPUT_DIR = args.output_dir

consumer_kwargs = {
    "bootstrap_servers": args.bootstrap_server,
    "auto_offset_reset": "earliest",
    "value_deserializer": lambda v: json.loads(v.decode("utf-8")) if v else None,
    "group_id": args.group_id,
}
if args.batch:
    consumer_kwargs["consumer_timeout_ms"] = args.timeout

consumer = None
for attempt in range(1, args.connect_retries + 1):
    try:
        candidate = KafkaConsumer(*TOPICS, **consumer_kwargs)
        consumer = candidate
        break
    except KafkaError as error:
        if attempt == args.connect_retries:
            raise RuntimeError(
                f"Unable to connect to Kafka at {args.bootstrap_server} "
                f"after {args.connect_retries} attempts: {error}"
            ) from error
        print(
            f"Kafka connection attempt {attempt}/{args.connect_retries} failed: {error}; "
            f"retrying in {args.retry_delay:g}s",
            file=sys.stderr,
        )
        time.sleep(args.retry_delay)

os.makedirs(OUTPUT_DIR, exist_ok=True)

if args.batch:
    print(f"Draining messages from topics: {TOPICS} (timeout {args.timeout}ms)...")
else:
    print(f"Listening on topics: {TOPICS}")
    print("Waiting for change events... (Ctrl+C to stop)\n")

for message in consumer:
    if message.value is None:
        continue  # tombstone record from a delete, skip for now

    table = message.topic.split(".")[-1]
    payload = message.value.get("payload", message.value)

    op = payload.get("op", "unknown")  # c=create, u=update, d=delete, r=snapshot read
    ts_ms = payload.get("ts_ms", 0)

    record = {
        "table": table,
        "operation": op,
        "timestamp_ms": ts_ms,
        "before": payload.get("before"),
        "after": payload.get("after"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    # One JSON file per event, organized by table - simple bronze layer pattern
    table_dir = os.path.join(OUTPUT_DIR, table)
    os.makedirs(table_dir, exist_ok=True)

    # Kafka offsets make the bronze write idempotent and prevent same-millisecond events from overwriting each other.
    filename = f"{ts_ms}_{op}_{message.partition}_{message.offset}.json"
    filepath = os.path.join(table_dir, filename)

    temp_filepath = f"{filepath}.{os.getpid()}.tmp"
    with open(temp_filepath, "w") as f:
        json.dump(record, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_filepath, filepath)

    print(f"[{table}] op={op} -> saved {filepath}")