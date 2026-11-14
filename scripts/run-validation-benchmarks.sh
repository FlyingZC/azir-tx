#!/usr/bin/env bash
# Copyright 2026 Azir TX contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

cd "$(dirname "$0")/.."

: "${AZIR_TX_CONTROL_JDBC:=jdbc:mysql://127.0.0.1:23306/control}"
: "${AZIR_TX_SHARD1_JDBC:=jdbc:mysql://127.0.0.1:23306/shard_1}"
: "${AZIR_TX_SHARD2_JDBC:=jdbc:mysql://127.0.0.1:23306/shard_2}"
: "${AZIR_TX_USERNAME:=root}"
: "${AZIR_TX_PASSWORD:=root}"
: "${AZIR_TX_BENCH_CONCURRENCY:=100}"
: "${AZIR_TX_BENCH_DURATION_SECONDS:=10}"
: "${AZIR_TX_FRESH_DOCKER:=true}"
: "${AZIR_TX_WAIT_SECONDS:=120}"

export AZIR_TX_CONTROL_JDBC
export AZIR_TX_SHARD1_JDBC
export AZIR_TX_SHARD2_JDBC
export AZIR_TX_USERNAME
export AZIR_TX_PASSWORD
export AZIR_TX_BENCH_CONCURRENCY
export AZIR_TX_BENCH_DURATION_SECONDS

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

export AZIR_TX_RESET=true
export AZIR_TX_TSO_MODE=DIRECT
export AZIR_TX_TSO_BATCH_SIZE=1
./mvnw -q -DskipTests exec:java -Dexec.mainClass=com.flyingzc.azir.e2e.TimestampOracleBenchmark

export AZIR_TX_RESET=true
export AZIR_TX_TSO_MODE=LEGACY_BATCH
export AZIR_TX_TSO_BATCH_SIZE=256
./mvnw -q -DskipTests exec:java -Dexec.mainClass=com.flyingzc.azir.e2e.TimestampOracleBenchmark

export AZIR_TX_RESET=true
export AZIR_TX_TSO_MODE=SEGMENT
export AZIR_TX_TSO_BATCH_SIZE=256
./mvnw -q -DskipTests exec:java -Dexec.mainClass=com.flyingzc.azir.e2e.TimestampOracleBenchmark

export AZIR_TX_RESET=true
export AZIR_TX_BUFFERED_MODE=false
export AZIR_TX_PARALLEL_PREWRITE=false
export AZIR_TX_PARALLEL_PREPARE=false
export AZIR_TX_PARALLEL_COMMIT=false
export AZIR_TX_PARALLEL_ROLLBACK=false
./mvnw -q -DskipTests exec:java -Dexec.mainClass=com.flyingzc.azir.e2e.CrossShardWritePathPrototypeBenchmark

export AZIR_TX_RESET=true
export AZIR_TX_BUFFERED_MODE=true
export AZIR_TX_PARALLEL_PREWRITE=true
export AZIR_TX_PARALLEL_PREPARE=true
export AZIR_TX_PARALLEL_COMMIT=true
export AZIR_TX_PARALLEL_ROLLBACK=true
./mvnw -q -DskipTests exec:java -Dexec.mainClass=com.flyingzc.azir.e2e.CrossShardWritePathPrototypeBenchmark

export AZIR_TX_RESET=true
export AZIR_TX_ASYNC_COMMIT=false
export AZIR_TX_SECONDARY_FINALIZE_WORKERS=8
./mvnw -q -DskipTests exec:java -Dexec.mainClass=com.flyingzc.azir.e2e.AsyncCommitPrototypeBenchmark

export AZIR_TX_RESET=true
export AZIR_TX_ASYNC_COMMIT=true
export AZIR_TX_SECONDARY_FINALIZE_WORKERS=8
./mvnw -q -DskipTests exec:java -Dexec.mainClass=com.flyingzc.azir.e2e.AsyncCommitPrototypeBenchmark
