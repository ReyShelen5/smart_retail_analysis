#!/usr/bin/env bash
# Run Hive (embedded beeline) with the project's config.
#   ./scripts/hive.sh                         -> interactive shell
#   ./scripts/hive.sh -f sql/01_xxx.sql       -> run a script
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
export HIVE_CONF_DIR="$PROJECT_DIR/conf"
mkdir -p warehouse tmp
exec beeline -u "jdbc:hive2://" -n "$USER" --silent=true \
     --showHeader=true --outputformat=table \
     --hivevar PROJECT_DIR="$PROJECT_DIR" "$@"
