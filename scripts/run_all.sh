#!/usr/bin/env bash
# Run the full batch pipeline: datasets -> metastore -> Hive tables -> analytics -> reports
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
mkdir -p reports tmp

if [[ ! -f data/historical/sales_history.csv ]]; then
  echo ">> Generating datasets"
  python3 scripts/generate_datasets.py
fi

if [[ ! -d metastore_db ]]; then
  echo ">> Initialising Hive metastore"
  HIVE_CONF_DIR="$PROJECT_DIR/conf" schematool -dbType derby -initSchema \
    > tmp/schematool.log 2>&1 || { tail -20 tmp/schematool.log; exit 1; }
fi

for f in sql/0[1-6]_*.sql; do
  echo ">> Running $f"
  ./scripts/hive.sh -f "$f" > "reports/$(basename "$f" .sql).out.txt"
done

if ls -d data/streaming/sale_date=* >/dev/null 2>&1; then
  echo ">> Running sql/07_streaming_analytics.sql"
  ./scripts/hive.sh -f sql/07_streaming_analytics.sql > reports/07_streaming_analytics.out.txt
else
  echo ">> Skipping streaming analytics (no data yet - run: docker compose up -d --build)"
fi

echo "Done. Query results are in reports/"
