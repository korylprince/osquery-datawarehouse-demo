#!/bin/bash
set -euo pipefail

TRINO_URL="${TRINO_URL:-trino.trino.svc.cluster.local:8080}"
CATALOG="iceberg"
SCHEMA="default"

run_sql() {
  trino --server "$TRINO_URL" --catalog "$CATALOG" --schema "$SCHEMA" --execute "$1" 2>/dev/null
}

tables=$(run_sql "SHOW TABLES LIKE 'osquery_%'" | tr -d '"')

if [ -z "$tables" ]; then
  echo "No osquery_ tables found"
  exit 0
fi

for table in $tables; do
  echo "=== Cleaning ${table} ==="

  echo "  Expiring old snapshots..."
  run_sql "ALTER TABLE \"${table}\" EXECUTE expire_snapshots(retention_threshold => '1d', clean_expired_metadata => true)" || echo "  WARN: expire_snapshots failed"

  echo "  Removing orphan files..."
  run_sql "ALTER TABLE \"${table}\" EXECUTE remove_orphan_files(retention_threshold => '1d')" || echo "  WARN: remove_orphan_files failed"

  echo "  Done: ${table}"
done

echo "Daily cleanup complete"
