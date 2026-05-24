#!/usr/bin/env bash
# Run osqueryd with the aws_billing_extension, a scheduled pack, serial
# decoration, and Kafka (mTLS) logging.
#
# Usage:
#   ./scripts/run_osquery.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/.."
TF_DIR="$REPO_ROOT/terraform"
EXT_DIR="$REPO_ROOT/osquery-extension"
OSQUERYD="/opt/osquery/lib/osquery.app/Contents/MacOS/osqueryd"

# ---------------------------------------------------------------------------
# 1. Bootstrap: get Kafka broker + mTLS certs from the cluster
# ---------------------------------------------------------------------------
BROKER=$(cd "$TF_DIR" && tofu output -raw kafka_broker)
if [ -z "$BROKER" ]; then
  echo "ERROR: kafka_broker output is empty - configure Cloudflare in terraform.tfvars" >&2
  exit 1
fi

TMPDIR=$(mktemp -d /tmp/osquery-kafka.XXXXXX)
trap 'rm -rf "$TMPDIR"' EXIT

# Pull client cert/key from the kafka namespace (same source as kafka_mtls.py)
"$SCRIPT_DIR/kubectl.sh" get secret kafka-client-cert -n kafka -o json \
  | python3 -c "
import json, sys, base64
data = json.load(sys.stdin)['data']
open('$TMPDIR/client.crt','wb').write(base64.b64decode(data['tls.crt']))
open('$TMPDIR/client.key','wb').write(base64.b64decode(data['tls.key']))
"
chmod 600 "$TMPDIR/client.key"

# ---------------------------------------------------------------------------
# 2. Build the extension if needed
# ---------------------------------------------------------------------------
EXT_BIN="$EXT_DIR/aws_billing_extension"
if [ ! -x "$EXT_BIN" ]; then
  echo "Building aws_billing_extension ..."
  pushd "$EXT_DIR" > /dev/null
  go build -o "$EXT_BIN" .
  popd > /dev/null
fi

# ---------------------------------------------------------------------------
# 3. Create osquery config (Kafka options + pack + decorations)
# ---------------------------------------------------------------------------
cat > "$TMPDIR/osquery.conf" <<EOF
{
  "options": {
    "logger_kafka_brokers": "ssl://${BROKER}",
    "logger_kafka_topic": "osquery",
    "logger_kafka_acks": "all"
  },
  "packs": {
    "aws_billing": {
      "queries": {
        "monthly_billing": {
          "query": "SELECT * FROM aws_billing;",
          "interval": 60,
          "snapshot": true,
          "description": "Pull monthly AWS blended cost from Cost Explorer"
        }
      }
    }
  },
  "decorators": {
    "load": [
      "SELECT 'demo@example.com' AS email, 'demo' AS serial"
    ]
  }
}
EOF

# ---------------------------------------------------------------------------
# 4. Launch osqueryd with extension
# ---------------------------------------------------------------------------
echo "Starting osqueryd → Kafka (ssl://$BROKER) ..."
exec "$OSQUERYD" \
  --logger_plugin kafka_producer \
  --tls_client_cert "$TMPDIR/client.crt" \
  --tls_client_key "$TMPDIR/client.key" \
  --config_path "$TMPDIR/osquery.conf" \
  --extensions_require aws_billing_extension \
  --extensions_socket "$TMPDIR/osquery.em" \
  --extensions_timeout 10 \
  --pidfile "$TMPDIR/osqueryd.pid" \
  --database_path "$TMPDIR/osquery.db" \
  --extension "$EXT_BIN"
