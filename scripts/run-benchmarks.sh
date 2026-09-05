#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

: "${AZIR_TX_CONTROL_JDBC:=jdbc:mysql://127.0.0.1:3306/control}"
: "${AZIR_TX_SHARD1_JDBC:=jdbc:mysql://127.0.0.1:3306/shard_1}"
: "${AZIR_TX_SHARD2_JDBC:=jdbc:mysql://127.0.0.1:3306/shard_2}"
: "${AZIR_TX_USERNAME:=root}"
: "${AZIR_TX_PASSWORD:=root}"
: "${AZIR_TX_BENCH_DURATION_SECONDS:=10}"
: "${AZIR_TX_CONFLICT_MODE:=WAIT}"
: "${AZIR_TX_LOCK_WAIT_TIMEOUT_MS:=5000}"
: "${AZIR_TX_LOCK_WAIT_BACKOFF_INITIAL_MS:=5}"
: "${AZIR_TX_LOCK_WAIT_BACKOFF_MAX_MS:=100}"
: "${AZIR_TX_LOCK_WAIT_BACKOFF_MULTIPLIER:=2.0}"
: "${AZIR_TX_LOCK_WAIT_BACKOFF_JITTER:=true}"
: "${AZIR_TX_RETRY_MAX_ATTEMPTS:=8}"
: "${AZIR_TX_RETRY_BACKOFF_INITIAL_MS:=5}"
: "${AZIR_TX_RETRY_BACKOFF_MAX_MS:=100}"
: "${AZIR_TX_RETRY_BACKOFF_MULTIPLIER:=2.0}"
: "${AZIR_TX_RETRY_BACKOFF_JITTER:=true}"
: "${AZIR_TX_FRESH_DOCKER:=true}"
: "${AZIR_TX_WAIT_SECONDS:=120}"

export AZIR_TX_CONTROL_JDBC
export AZIR_TX_SHARD1_JDBC
export AZIR_TX_SHARD2_JDBC
export AZIR_TX_USERNAME
export AZIR_TX_PASSWORD
export AZIR_TX_CONFLICT_MODE
export AZIR_TX_LOCK_WAIT_TIMEOUT_MS
export AZIR_TX_LOCK_WAIT_BACKOFF_INITIAL_MS
export AZIR_TX_LOCK_WAIT_BACKOFF_MAX_MS
export AZIR_TX_LOCK_WAIT_BACKOFF_MULTIPLIER
export AZIR_TX_LOCK_WAIT_BACKOFF_JITTER
export AZIR_TX_RETRY_MAX_ATTEMPTS
export AZIR_TX_RETRY_BACKOFF_INITIAL_MS
export AZIR_TX_RETRY_BACKOFF_MAX_MS
export AZIR_TX_RETRY_BACKOFF_MULTIPLIER
export AZIR_TX_RETRY_BACKOFF_JITTER

if [[ "${AZIR_TX_FRESH_DOCKER}" == "true" ]]; then
  docker compose down -v --remove-orphans || true
fi

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

run_case() {
  local concurrency="$1"
  local conflict="$2"
  export AZIR_TX_RESET=true
  export AZIR_TX_BENCH_CONCURRENCY="$concurrency"
  export AZIR_TX_BENCH_DURATION_SECONDS
  export AZIR_TX_BENCH_CONFLICT="$conflict"
  mvn -q -DskipTests exec:java -Dexec.mainClass=mvcc.e2e.MysqlTxnBenchmark
}

for c in 50 100 200; do
  run_case "$c" false
done

for c in 50 100 200; do
  run_case "$c" true
done
