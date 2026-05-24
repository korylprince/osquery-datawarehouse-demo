#!/usr/bin/env python3
"""Generate synthetic osquery os_version events and stream them to Kafka.

Simulates 10 000 devices over April 1 – June 25, 2026, producing one snapshot
event per active tick (hourly, within a per-device 09:00–18:00 local window
with daily jitter) and sending each event to Kafka as it is generated.

Usage:
    python scripts/os_update_scenario.py [--workers N]
"""

import argparse
import json
import math
import multiprocessing
import os
import random
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import Producer
from kafka_mtls import get_mtls_config

# ---------------------------------------------------------------------------
# Upgrade probability parameters - adjust to tune rollout behaviour
# ---------------------------------------------------------------------------
P_FREE = 0.001      # hourly upgrade chance for a version with no enforcement date
P_MIN = 0.001        # floor probability just after release
P_MAX_PRE = 0.75     # ceiling probability approaching enforcement day
LAMBDA = 0.03         # exponential growth rate across the enforcement window
P_POST = 0.5        # hourly catch-up probability after the enforcement date passes

# ---------------------------------------------------------------------------
# Jitter parameters
# ---------------------------------------------------------------------------
JITTER_WINDOW_SIGMA_SEC = 3600  # σ for active-window open/close shift (±1 hour)
JITTER_TICK_SIGMA_SEC = 600     # σ for per-tick send-time offset (±10 minutes)

WINDOW_OPEN_SEC = 9 * 3600    # nominal 09:00 in device-local time
WINDOW_CLOSE_SEC = 18 * 3600  # nominal 18:00 in device-local time

# ---------------------------------------------------------------------------
# Real-world absence probabilities - evaluated once per device per local day
# ---------------------------------------------------------------------------
P_PTO = 0.05          # chance that the device user is on PTO (whole local day skipped)
P_WEEKEND_SKIP = 0.80 # chance that the device doesn't check in on a Saturday/Sunday
P_MISS_TICK = 0.02    # chance that an individual active check-in is silently dropped

# ---------------------------------------------------------------------------
# OS version timeline - ordered oldest → newest
# ---------------------------------------------------------------------------
VERSIONS = [
    {
        "version": "26.3.2",
        "release": None,
        "enforcement": datetime(2026, 4, 1, tzinfo=timezone.utc),
    },
    {
        "version": "26.4",
        "release": datetime(2026, 3, 24, tzinfo=timezone.utc),
        "enforcement": None,
    },
    {
        "version": "26.4.1",
        "release": datetime(2026, 4, 9, tzinfo=timezone.utc),
        "enforcement": datetime(2026, 4, 25, tzinfo=timezone.utc),
    },
    {
        "version": "26.5",
        "release": datetime(2026, 5, 11, tzinfo=timezone.utc),
        "enforcement": datetime(2026, 5, 27, tzinfo=timezone.utc),
    },
    {
        "version": "26.5.1",
        "release": datetime(2026, 6, 1, tzinfo=timezone.utc),
        "enforcement": datetime(2026, 6, 25, tzinfo=timezone.utc),
    },
]

VERSION_INDEX = {v["version"]: i for i, v in enumerate(VERSIONS)}

# ---------------------------------------------------------------------------
# Initial version distribution at simulation start (April 1, 2026)
# ---------------------------------------------------------------------------
INITIAL_VERSIONS = ["26.4", "26.3.2"]
INITIAL_WEIGHTS = [0.97, 0.03]

# ---------------------------------------------------------------------------
# Simulation period
# ---------------------------------------------------------------------------
SIM_START = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
SIM_END = datetime(2026, 6, 25, 23, 59, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Kafka config
# ---------------------------------------------------------------------------
TOPIC = "osquery"

TF_DIR = Path(__file__).resolve().parent.parent / "terraform"
REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_tz_offset(tz_str: str) -> int:
    """Parse '+HH:MM' or '-HH:MM' into a total-seconds offset."""
    sign = 1 if tz_str[0] == "+" else -1
    h, m = tz_str[1:].split(":")
    return sign * (int(h) * 3600 + int(m) * 60)


def upgrade_probability(v_info: dict, tick_dt: datetime, lambda_factor: float) -> float:
    """Return the per-tick upgrade probability p(V, t) for a given version."""
    release = v_info["release"]
    enforcement = v_info["enforcement"]

    if enforcement is None:
        return P_FREE

    if tick_dt < release:
        return 0.0

    if tick_dt < enforcement:
        span = (enforcement - release).total_seconds()
        f = (tick_dt - release).total_seconds() / span
        effective_lambda = LAMBDA * lambda_factor
        return P_MIN + (P_MAX_PRE - P_MIN) * (1.0 - math.exp(-effective_lambda * f))

    return P_POST


def get_kafka_broker() -> str:
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


def delivery_callback(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)



# ---------------------------------------------------------------------------
# Worker: simulate a slice of devices across the full time range
# ---------------------------------------------------------------------------

def simulate_chunk(args) -> int:
    chunk_id, devices_chunk, broker = args

    prefix = f"[W{chunk_id}]"
    rng = random.Random()

    producer = Producer(
        get_mtls_config(
            broker,
            extra_config={
                "client.id": f"{socket.gethostname()}-w{chunk_id}",
                "compression.codec": "lz4",
                "acks": "all",
                "queue.buffering.max.messages": 200_000,
                "queue.buffering.max.kbytes": 524_288,
                "batch.num.messages": 100_000,
                "linger.ms": 50,
            },
        )
    )

    version_lambda_factor = {
        v["version"]: rng.uniform(0.8, 1.2) for v in VERSIONS
    }

    serials = [dev["serial"] for dev in devices_chunk]
    tz_offset_sec = {
        dev["serial"]: parse_tz_offset(dev["timezone_offset"]) for dev in devices_chunk
    }
    emails = {dev["serial"]: dev["email"] for dev in devices_chunk}
    # Pre-encode keys once to avoid per-message allocation
    msg_keys = {serial: f"{serial}_osqueryd".encode() for serial in serials}
    current_version = {
        serial: rng.choices(INITIAL_VERSIONS, weights=INITIAL_WEIGHTS, k=1)[0]
        for serial in serials
    }
    counter = {serial: 0 for serial in serials}

    device_local_day = {serial: None for serial in serials}
    device_day_flags = {}

    last_utc_day = None
    day_lambda_factor = 1.0

    total_events = 0
    start = time.monotonic()
    last_log = start
    since_poll = 0

    tick = SIM_START
    while tick <= SIM_END:
        utc_day = tick.date()
        if utc_day != last_utc_day:
            day_lambda_factor = rng.uniform(0.8, 1.2)
            last_utc_day = utc_day

        tick_unix = int(tick.timestamp())

        for serial in serials:
            tz_off = tz_offset_sec[serial]

            local_date = datetime.fromtimestamp(
                tick_unix + tz_off, tz=timezone.utc
            ).date()

            if local_date != device_local_day[serial]:
                device_local_day[serial] = local_date
                is_weekend = local_date.weekday() >= 5
                skip_day = rng.random() < P_PTO
                if not skip_day and is_weekend:
                    skip_day = rng.random() < P_WEEKEND_SKIP
                device_day_flags[serial] = {
                    "skip": skip_day,
                    "open": rng.gauss(0, JITTER_WINDOW_SIGMA_SEC),
                    "close": rng.gauss(0, JITTER_WINDOW_SIGMA_SEC),
                    "tick": rng.gauss(0, JITTER_TICK_SIGMA_SEC),
                }

            flags = device_day_flags[serial]

            if flags["skip"]:
                continue

            local_sec_of_day = (tick_unix + tz_off) % 86400
            window_open = WINDOW_OPEN_SEC + flags["open"]
            window_close = WINDOW_CLOSE_SEC + flags["close"]
            if not (window_open <= local_sec_of_day < window_close):
                continue

            # Upgrade algorithm
            cur_idx = VERSION_INDEX[current_version[serial]]
            for v_info in reversed(VERSIONS):
                v_idx = VERSION_INDEX[v_info["version"]]
                if v_idx <= cur_idx:
                    continue
                p = upgrade_probability(
                    v_info, tick,
                    day_lambda_factor * version_lambda_factor[v_info["version"]]
                )
                if p > 0 and rng.random() < p:
                    current_version[serial] = v_info["version"]
                    cur_idx = v_idx
                    break

            if rng.random() < P_MISS_TICK:
                continue

            # Build and emit event
            send_unix = tick_unix + int(flags["tick"])
            counter[serial] += 1
            send_dt = datetime.fromtimestamp(send_unix, tz=timezone.utc)
            record = json.dumps(
                {
                    "action": "snapshot",
                    "snapshot": [{"version": current_version[serial]}],
                    "name": "os_version",
                    "hostIdentifier": serial,
                    "calendarTime": send_dt.strftime("%a %b %d %H:%M:%S %Y UTC"),
                    "unixTime": str(send_unix),
                    "epoch": "0",
                    "counter": str(counter[serial]),
                    "numerics": False,
                    "decorations": {
                        "serial": serial,
                        "email": emails[serial],
                    },
                },
                separators=(",", ":"),
            ).encode()

            try:
                producer.produce(
                    TOPIC,
                    key=msg_keys[serial],
                    value=record,
                    callback=delivery_callback,
                )
            except BufferError:
                producer.poll(1)
                try:
                    producer.produce(
                        TOPIC,
                        key=msg_keys[serial],
                        value=record,
                        callback=delivery_callback,
                    )
                except BufferError:
                    print(f"  {prefix} Buffer full, dropping message", file=sys.stderr)
                    continue

            total_events += 1
            since_poll += 1
            if since_poll >= 1000:
                producer.poll(0)
                since_poll = 0

        elapsed = time.monotonic() - last_log
        if elapsed >= 5:
            rate = total_events / (time.monotonic() - start)
            print(f"  {prefix} count={total_events:,}  rate={rate:,.0f} msg/s", flush=True)
            last_log = time.monotonic()

        tick += timedelta(hours=1)

    producer.flush()
    print(f"  {prefix} Done. Sent {total_events:,} events.", flush=True)
    return total_events


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic os_version osquery events and stream to Kafka."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(8, os.cpu_count() or 4),
        help="Number of parallel worker processes (default: min(8, cpu_count))",
    )
    args = parser.parse_args()

    # Load devices
    devices_path = REPO_ROOT / "synthetic_data" / "synthetic_devices.json"
    with open(devices_path) as fh:
        devices = json.load(fh)
    print(f"Loaded {len(devices):,} devices.", flush=True)

    # Connect to Kafka (resolve broker once; workers reuse the address)
    broker = get_kafka_broker()
    print(f"Producing to {broker}  topic={TOPIC}", flush=True)

    n_workers = min(args.workers, len(devices))
    chunk_size = math.ceil(len(devices) / n_workers)
    chunks = [devices[i:i + chunk_size] for i in range(0, len(devices), chunk_size)]
    n_workers = len(chunks)

    print(f"Starting {n_workers} workers with ~{chunk_size} devices each...", flush=True)
    start = time.monotonic()

    with multiprocessing.Pool(processes=n_workers) as pool:
        results = pool.map(
            simulate_chunk,
            [(i, chunk, broker) for i, chunk in enumerate(chunks)],
        )

    total = sum(results)
    elapsed = time.monotonic() - start
    print(
        f"Simulation complete. Sent {total:,} events in {elapsed:.1f}s "
        f"({total / elapsed:,.0f} msg/s avg)."
    )


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()

