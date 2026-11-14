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
: "${AZIR_TX_RESET:=true}"
: "${AZIR_TX_WAIT_SECONDS:=120}"
: "${AZIR_TX_FRESH_DOCKER:=true}"

export AZIR_TX_CONTROL_JDBC
export AZIR_TX_SHARD1_JDBC
export AZIR_TX_SHARD2_JDBC
export AZIR_TX_USERNAME
export AZIR_TX_PASSWORD
export AZIR_TX_RESET

echo "[1/4] starting mysql via docker compose"
if [[ "${AZIR_TX_FRESH_DOCKER}" == "true" ]]; then
  echo "cleaning previous docker compose environment"
  docker compose down -v --remove-orphans || true
fi
docker compose up -d

echo "[2/4] waiting for mysql health"
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

echo "[3/4] running ./mvnw test"
./mvnw test

echo "[4/4] running real mysql e2e"
./scripts/run-mysql-e2e.sh

echo "ALL TESTS PASS"
