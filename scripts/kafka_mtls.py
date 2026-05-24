"""Shared helper for Kafka mTLS client certificate management.

Automatically pulls the client cert and key from the cluster via kubectl
and writes them to a temporary directory that is cleaned up on exit.

The broker cert is the publicly-trusted Let's Encrypt cert
(`ingress-tls-pkcs8`), so no CA download is needed - the system CA
store already trusts it.

Usage:
    from kafka_mtls import get_mtls_config

    config = get_mtls_config(broker)
    # config is a dict ready for confluent_kafka Producer/Consumer:
    #   {"bootstrap.servers": broker,
    #    "security.protocol": "ssl",
    #    "ssl.certificate.location": "/tmp/.../client.crt",
    #    "ssl.key.location": "/tmp/.../client.key"}
"""

import atexit
import base64
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
_TF_DIR = _SCRIPTS_DIR.parent / "terraform"

# Track temp dirs for cleanup
_temp_dirs = []


def _cleanup() -> None:
    for d in _temp_dirs:
        try:
            shutil.rmtree(d, ignore_errors=True)
        except OSError:
            pass


atexit.register(_cleanup)


def _get_hostname() -> str:
    """Resolve the cluster hostname from OpenTofu outputs."""
    result = subprocess.run(
        ["tofu", "output", "-raw", "cloudflare_root_record"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        cwd=str(_TF_DIR),
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or "no stderr"
        raise RuntimeError(f"tofu output failed: {stderr}")
    hostname = result.stdout.strip()
    if not hostname:
        raise RuntimeError(
            "cloudflare_root_record is empty - ensure Cloudflare is configured in terraform.tfvars"
        )
    return hostname


def _kubectl_get_secret(namespace: str, name: str, key: str) -> bytes:
    """Fetch a secret value from the cluster via SSH + k3s kubectl."""
    hostname = _get_hostname()
    result = subprocess.run(
        [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "LogLevel=ERROR",
            f"ubuntu@{hostname}",
            "sudo k3s kubectl get secret", name, "-n", namespace, "-o", "json",
        ],
        capture_output=True,
        timeout=30,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip() or "no stderr"
        raise RuntimeError(
            f"kubectl get secret {namespace}/{name} failed: {stderr}"
        )
    import json
    data = json.loads(result.stdout)
    b64_data = data.get("data", {}).get(key, "")
    if not b64_data:
        raise RuntimeError(f"Secret {namespace}/{name} key '{key}' is empty")
    return base64.b64decode(b64_data)


def get_mtls_config(broker: str, extra_config: dict | None = None) -> dict:
    """Return a confluent_kafka config dict with mTLS enabled.

    Pulls the client cert and key from the cluster into a temp directory
    and returns paths suitable for ssl.certificate.location etc.

    The broker cert is Let's Encrypt (publicly trusted), so no CA cert
    download is needed - the system CA store handles broker verification.

    Args:
        broker: Kafka bootstrap server (e.g. "kafka.example.com:9093")
        extra_config: optional extra keys to merge into the result

    Returns:
        Dict ready for confluent_kafka Producer or Consumer constructor.
    """
    tmpdir = tempfile.mkdtemp(prefix="kafka-mtls-")
    _temp_dirs.append(tmpdir)

    client_cert_path = Path(tmpdir) / "client.crt"
    client_key_path = Path(tmpdir) / "client.key"

    # Download client cert and key (tls.crt / tls.key in kafka-client-cert)
    client_cert_data = _kubectl_get_secret("kafka", "kafka-client-cert", "tls.crt")
    client_cert_path.write_bytes(client_cert_data)

    client_key_data = _kubectl_get_secret("kafka", "kafka-client-cert", "tls.key")
    client_key_path.write_bytes(client_key_data)
    os.chmod(client_key_path, 0o600)

    config = {
        "bootstrap.servers": broker,
        "security.protocol": "ssl",
        "ssl.certificate.location": str(client_cert_path),
        "ssl.key.location": str(client_key_path),
    }
    if extra_config:
        config.update(extra_config)
    return config
