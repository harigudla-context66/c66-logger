#!/bin/sh
# Create the Kafka topics EventLogger publishes to. Safe to run again.
#
#   BOOTSTRAP=broker:9092 PARTITIONS=12 REPLICATION_FACTOR=3 sh deploy/kafka-topics.sh
#
# Kafka only buffers events on their way to ClickHouse, so topic retention is
# short (7 days: enough to replay after a ClickHouse outage). How long events are
# kept is decided by RetentionDays in ClickHouse, not here.
set -e
BOOTSTRAP=${BOOTSTRAP:-localhost:9092}
PARTITIONS=${PARTITIONS:-6}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-1}
RETENTION_MS=${RETENTION_MS:-604800000}
KAFKA_BIN=${KAFKA_BIN:-/opt/kafka/bin}

for topic in c66.events.telemetry c66.events.observability c66.events.audit c66.events.alerts; do
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION_FACTOR" \
    --config retention.ms="$RETENTION_MS"
done
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list
