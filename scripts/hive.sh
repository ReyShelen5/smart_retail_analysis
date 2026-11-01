#!/usr/bin/env bash
# Run Hive (embedded beeline) with the project's config.
#   ./scripts/hive.sh                     -> interactive shell
#   ./scripts/hive.sh -f sql/0X_xxx.sql   -> run a script
# ${hivevar:PROJECT_DIR} in SQL files is replaced with the project path.
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
export HIVE_CONF_DIR="$PROJECT_DIR/conf"
unset HADOOP_PREFIX
# Java 17: let in-process MapReduce access JDK internals
JAVA17_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.regex=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.sql/java.sql=ALL-UNNAMED"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} $JAVA17_OPENS"
export HADOOP_OPTS="${HADOOP_OPTS:-} $JAVA17_OPENS"
mkdir -p warehouse tmp/rendered

args=()
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-f" && $# -ge 2 ]]; then
    rendered="tmp/rendered/$(basename "$2")"
    sed "s#\${hivevar:PROJECT_DIR}#${PROJECT_DIR}#g" "$2" > "$rendered"
    args+=(-f "$rendered"); shift 2
  else
    args+=("$1"); shift
  fi
done

exec beeline -u "jdbc:hive2://" -n "$USER" --silent=true \
     --showHeader=true --outputformat=table "${args[@]}"
