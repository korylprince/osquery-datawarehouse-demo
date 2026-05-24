#!/usr/bin/env python3
"""Kafka consumer that reads osquery events and prints them to stdout."""

import json
import signal
import socket
import subprocess
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_END
from kafka_mtls import get_mtls_config

TF_DIR = Path(__file__).resolve().parent.parent / "terraform"


def get_kafka_broker():
    """Read the external Kafka broker address from OpenTofu outputs."""
    result = subprocess.run(
        ["tofu", "output", "-raw", "kafka_broker"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        cwd=str(TF_DIR),
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or "no stderr"
        raise RuntimeError(f"tofu output failed: {stderr}")
    broker = result.stdout.strip()
    if not broker:
        raise RuntimeError(
            "kafka_broker output is empty - ensure Cloudflare is configured in terraform.tfvars"
        )
    return broker


BROKER = get_kafka_broker()
TOPIC = "osquery"
GROUP_ID = f"osquery-consumer-{socket.gethostname()}"


def main():
    consumer = Consumer(
        get_mtls_config(
            BROKER,
            extra_config={
                "group.id": GROUP_ID,
                "client.id": socket.gethostname(),
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            },
        )
    )

    # seek to end before consuming
    def on_assign(consumer, partitions):
        for p in partitions:
            p.offset = OFFSET_END
        consumer.assign(partitions)

    consumer.subscribe([TOPIC], on_assign=on_assign)

    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"Consuming from {BROKER} topic={TOPIC} group={GROUP_ID} (Ctrl+C to stop)")
    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                print(json.dumps(value, indent=2))
            except (json.JSONDecodeError, UnicodeDecodeError):
                print(f"Raw message: {msg.value()}", file=sys.stderr)
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
