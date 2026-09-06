#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

: "${AZIR_TX_CONTROL_JDBC:=jdbc:mysql://127.0.0.1:23306/control}"
: "${AZIR_TX_SHARD1_JDBC:=jdbc:mysql://127.0.0.1:23306/shard_1}"
: "${AZIR_TX_SHARD2_JDBC:=jdbc:mysql://127.0.0.1:23306/shard_2}"
: "${AZIR_TX_USERNAME:=root}"
: "${AZIR_TX_PASSWORD:=root}"
: "${AZIR_TX_RESET:=true}"

export AZIR_TX_CONTROL_JDBC
export AZIR_TX_SHARD1_JDBC
export AZIR_TX_SHARD2_JDBC
export AZIR_TX_USERNAME
export AZIR_TX_PASSWORD
export AZIR_TX_RESET

echo "AZIR_TX_CONTROL_JDBC=$AZIR_TX_CONTROL_JDBC"
echo "AZIR_TX_SHARD1_JDBC=$AZIR_TX_SHARD1_JDBC"
echo "AZIR_TX_SHARD2_JDBC=$AZIR_TX_SHARD2_JDBC"

mvn -q -DskipTests exec:java -Dexec.mainClass=mvcc.e2e.MysqlMvccE2E
