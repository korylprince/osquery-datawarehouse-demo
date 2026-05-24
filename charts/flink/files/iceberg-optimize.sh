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
  echo "Optimizing ${table}..."
  run_sql "ALTER TABLE \"${table}\" EXECUTE optimize(file_size_threshold => '256MB')" || echo "  WARN: optimize failed for ${table}"
done

echo "Hourly optimization complete"
