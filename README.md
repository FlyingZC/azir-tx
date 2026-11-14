# Azir TX

A Java 21 transaction framework for independent MySQL shards, with global
Read Committed and Repeatable Read, snapshot reads, recovery and version GC.

The repository provides three explicit runtime choices:

- The default coordinator uses transaction metadata, intents and committed versions.
- Percolator uses a primary key's durable decision and recovers pending secondaries.
- The WAL runtime records complete write sets; the optional WAL HA module replicates
  decisions with Apache Ratis.

These runtimes have separate data layouts and commit identities. Selecting another
runtime does not migrate existing data. Production acceptance is still in progress.

## Requirements

Java 21 and Docker are required for the full integration tests. The Maven Wrapper
downloads Maven 3.9.9. MySQL tests use Testcontainers and isolated databases.

## Build and test

```sh
./mvnw clean verify
./mvnw -f wal-ha-reactor/pom.xml clean verify
```

The second command builds the core and WAL HA modules together. Tests include
process and container fault scenarios and may take several minutes.

## Run the MySQL example

The Compose service is a local development MySQL instance on port 23306 with
development-only `root` credentials. Use the dedicated example databases.

```sh
docker compose up -d
./mvnw compile exec:java -Dexec.mainClass=com.flyingzc.azir.e2e.MysqlMvccE2E
```

The example initializes its protocol tables and resets its example data. Override
the `AZIR_TX_CONTROL_JDBC`, `AZIR_TX_SHARD1_JDBC`, `AZIR_TX_SHARD2_JDBC`,
`AZIR_TX_USERNAME` and `AZIR_TX_PASSWORD` environment variables as needed.

## WAL node distribution

```sh
./mvnw -f wal-ha-reactor/pom.xml -Pdistribution -DskipTests package
```

The archive is written under `wal-ha/target/`. Its `conf/` directory contains an
example node configuration. Set peer addresses, database identities, TLS material
and local paths for your deployment before starting `bin/azir-wal-node`.

## Code style

```sh
./mvnw -f wal-ha-reactor/pom.xml spotless:apply
./mvnw -f wal-ha-reactor/pom.xml validate
```

The Java formatter and Checkstyle rules derive from Apache ShardingSphere 5.5.3.
Configuration and migration exceptions are under `build/`. A line-scoped baseline
covers existing complexity, declaration and fault-handling conventions. The full
upstream rules apply to new files and other lines; review the baseline when editing
affected code. Formatting, license and Javadoc checks are enabled.

## License

Apache License, Version 2.0. See `LICENSE` and `NOTICE`.
