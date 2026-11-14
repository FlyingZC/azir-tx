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

export AZIR_TX_CONTROL_JDBC
export AZIR_TX_SHARD1_JDBC
export AZIR_TX_SHARD2_JDBC
export AZIR_TX_USERNAME
export AZIR_TX_PASSWORD
export AZIR_TX_RESET

echo "AZIR_TX_CONTROL_JDBC=$AZIR_TX_CONTROL_JDBC"
echo "AZIR_TX_SHARD1_JDBC=$AZIR_TX_SHARD1_JDBC"
echo "AZIR_TX_SHARD2_JDBC=$AZIR_TX_SHARD2_JDBC"

./mvnw -q -DskipTests exec:java -Dexec.mainClass=com.flyingzc.azir.e2e.MysqlMvccE2E
