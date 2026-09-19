#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

: "${JAVA_HOME:=/Users/zc/0zc/01-my/01-soft/jdk-21.0.10+7/Contents/Home}"
: "${AZIR_TX_BENCH_DURATION_SECONDS:=5}"
: "${AZIR_TX_JDBC_MAX_POOL_SIZE:=256}"
: "${AZIR_TX_BENCH_CONCURRENCY_LIST:=50 100 200}"
: "${AZIR_TX_WAIT_SECONDS:=120}"

export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
export AZIR_TX_BENCH_DURATION_SECONDS
export AZIR_TX_JDBC_MAX_POOL_SIZE

docker compose up -d

deadline=$((SECONDS + AZIR_TX_WAIT_SECONDS))
while true; do
  status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' azir-tx-mysql 2>/dev/null || true)"
  if [[ "$status" == "healthy" ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    echo "mysql did not become healthy within ${AZIR_TX_WAIT_SECONDS}s"
    docker compose ps
    exit 1
  fi
  sleep 2
done

mvn -q -DskipTests compile

for concurrency in ${AZIR_TX_BENCH_CONCURRENCY_LIST}; do
  direct_line="$(
    AZIR_TX_RESET=true \
    AZIR_TX_BENCH_CONCURRENCY="$concurrency" \
    AZIR_TX_BENCH_CONFLICT=false \
    mvn -q -DskipTests exec:java -Dexec.mainClass=mvcc.e2e.MysqlDirectTwoShardTxnBenchmark \
      | grep 'DIRECT_TWO_SHARD_BENCH'
  )"
  mvcc_line="$(
    AZIR_TX_RESET=true \
    AZIR_TX_BENCH_CONCURRENCY="$concurrency" \
    AZIR_TX_BENCH_CONFLICT=false \
    mvn -q -DskipTests exec:java -Dexec.mainClass=mvcc.e2e.MysqlTxnBenchmark \
      | grep '^BENCH '
  )"
  direct_tps="$(printf '%s\n' "$direct_line" | sed -n 's/.*commit_tps=\([0-9.]*\).*/\1/p')"
  mvcc_tps="$(printf '%s\n' "$mvcc_line" | sed -n 's/.*commit_tps=\([0-9.]*\).*/\1/p')"
  loss="$(awk -v direct="$direct_tps" -v mvcc="$mvcc_tps" 'BEGIN { if (direct <= 0) print "NaN"; else printf "%.2f", (direct - mvcc) * 100.0 / direct }')"
  echo "$direct_line"
  echo "$mvcc_line"
  echo "LOSS concurrency=${concurrency} direct_commit_tps=${direct_tps} mvcc_commit_tps=${mvcc_tps} loss_percent=${loss}"
done
