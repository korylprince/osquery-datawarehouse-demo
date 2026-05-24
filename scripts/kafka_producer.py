#!/usr/bin/env python3
"""Mock osquery Kafka producer for scheduled query results."""

import json
import signal
import socket
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from confluent_kafka import Producer
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
HOSTNAME = socket.gethostname()
MSG_KEY = f"{HOSTNAME}_osqueryd".encode("utf-8")


class QueryError(Exception):
    """Raised when an osquery invocation fails."""


@dataclass(frozen=True)
class ScheduledQuery:
    name: str
    sql: str
    interval_seconds: int
    mode: str = "event"
    timeout_seconds: int = 30


@dataclass
class QueryState:
    counter: int = 0
    next_run: float = 0.0
    row_counts: Counter = field(default_factory=Counter)
    rows_by_key: dict[str, dict[str, str]] = field(default_factory=dict)


SCHEDULED_QUERIES = (
    ScheduledQuery(name="uptime", sql="select * from uptime;", interval_seconds=1, timeout_seconds=5),
    ScheduledQuery(name="battery", sql="select * from battery;", interval_seconds=60, timeout_seconds=10),
    ScheduledQuery(name="apps", sql="select * from apps;", interval_seconds=600, mode="snapshot", timeout_seconds=60),
)


def stringify_row(row):
    if not isinstance(row, dict):
        raise QueryError(f"expected row object, got {type(row).__name__}")
    return {str(key): "" if value is None else str(value) for key, value in row.items()}


def canonicalize_row(row):
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def run_osquery(sql, timeout_seconds):
    result = subprocess.run(
        ["osqueryi", "--json", sql],
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or "no stderr"
        raise QueryError(f"osqueryi exited {result.returncode}: {stderr}")

    try:
        rows = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise QueryError(f"invalid JSON: {exc}") from exc

    if not isinstance(rows, list):
        raise QueryError(f"expected result list, got {type(rows).__name__}")

    return [stringify_row(row) for row in rows]


def get_fallback_uptime_row():
    try:
        if sys.platform == "darwin":
            result = subprocess.run(
                ["sysctl", "-n", "kern.boottime"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            if result.returncode != 0:
                stderr = result.stderr.strip() or "no stderr"
                raise QueryError(f"sysctl exited {result.returncode}: {stderr}")

            parts = result.stdout.split("sec = ", 1)
            if len(parts) != 2:
                raise QueryError(f"unexpected kern.boottime format: {result.stdout.strip()}")

            boot_seconds = int(parts[1].split(",", 1)[0].strip())
            uptime_seconds = max(0, int(time.time()) - boot_seconds)
        else:
            with open("/proc/uptime", "r", encoding="utf-8") as handle:
                uptime_seconds = int(float(handle.read().split()[0]))
    except (OSError, ValueError, QueryError) as exc:
        raise QueryError(f"uptime fallback failed: {exc}") from exc

    return {
        "days": str(uptime_seconds // 86400),
        "hours": str((uptime_seconds % 86400) // 3600),
        "minutes": str((uptime_seconds % 3600) // 60),
        "seconds": str(uptime_seconds % 60),
        "total_seconds": str(uptime_seconds),
    }


def fetch_rows(query):
    try:
        return run_osquery(query.sql, timeout_seconds=query.timeout_seconds)
    except (subprocess.SubprocessError, OSError, QueryError, ValueError) as exc:
        if query.name != "uptime":
            raise QueryError(str(exc)) from exc
        print(f"uptime query failed, using fallback: {exc}", file=sys.stderr)
        return [get_fallback_uptime_row()]


def build_base_payload(query_name, counter):
    return {
        "name": query_name,
        "hostIdentifier": HOSTNAME,
        "calendarTime": time.strftime("%a %b %d %H:%M:%S %Y UTC", time.gmtime()),
        "unixTime": str(int(time.time())),
        "epoch": "0",
        "counter": str(counter),
        "numerics": False,
    }


def build_event_payloads(query, state, rows):
    payload_base = build_base_payload(query.name, state.counter)
    current_counts = Counter()
    current_rows_by_key = {}

    for row in rows:
        key = canonicalize_row(row)
        current_counts[key] += 1
        current_rows_by_key[key] = row

    payloads = []
    for key in sorted(state.row_counts):
        previous_count = state.row_counts[key]
        current_count = current_counts[key]
        if previous_count > current_count:
            row = state.rows_by_key[key]
            for _ in range(previous_count - current_count):
                payloads.append(json.dumps({
                    "action": "removed",
                    "columns": row,
                    **payload_base,
                }))

    for key in sorted(current_counts):
        previous_count = state.row_counts[key]
        current_count = current_counts[key]
        if current_count > previous_count:
            row = current_rows_by_key[key]
            for _ in range(current_count - previous_count):
                payloads.append(json.dumps({
                    "action": "added",
                    "columns": row,
                    **payload_base,
                }))

    state.row_counts = current_counts
    state.rows_by_key = current_rows_by_key
    state.counter += 1
    return payloads


def build_snapshot_payload(query, state, rows):
    payload = json.dumps({
        "action": "snapshot",
        "snapshot": rows,
        **build_base_payload(query.name, state.counter),
    })
    state.counter += 1
    return payload


def delivery_callback(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def main():
    producer = Producer(
        get_mtls_config(
            BROKER,
            extra_config={
                "client.id": HOSTNAME,
                "compression.codec": "none",
                "acks": "all",
            },
        )
    )

    running = True
    states = {query.name: QueryState() for query in SCHEDULED_QUERIES}

    def stop(signum, frame):
        del signum, frame
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"Producing to {BROKER} topic={TOPIC} (Ctrl+C to stop)")
    while running:
        now = time.monotonic()

        for query in SCHEDULED_QUERIES:
            state = states[query.name]
            if now < state.next_run:
                continue

            try:
                rows = fetch_rows(query)
            except QueryError as exc:
                print(f"{query.name} query failed: {exc}", file=sys.stderr)
                state.next_run = now + query.interval_seconds
                continue

            if query.mode == "snapshot":
                payloads = [build_snapshot_payload(query, state, rows)]
            else:
                payloads = build_event_payloads(query, state, rows)

            for payload in payloads:
                producer.produce(
                    TOPIC,
                    key=MSG_KEY,
                    value=payload.encode("utf-8"),
                    callback=delivery_callback,
                )
                producer.poll(0)

            state.next_run = now + query.interval_seconds

        producer.poll(0)
        time.sleep(1)

    print("Flushing...")
    producer.flush(timeout=3)
    print("Done.")


if __name__ == "__main__":
    main()
