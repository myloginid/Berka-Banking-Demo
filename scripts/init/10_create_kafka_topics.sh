#!/usr/bin/env bash
#
# Create Kafka topics for Berka streaming data.
# Usage:
#   BROKERS="kafka1:9092" ./10_create_kafka_topics.sh

set -euo pipefail

BROKERS="${BROKERS:-localhost:9092}"

echo "Using Kafka brokers: ${BROKERS}"

kafka-topics.sh --bootstrap-server "${BROKERS}" --create \
  --topic berka_loans \
  --partitions 6 --replication-factor 3 || true

kafka-topics.sh --bootstrap-server "${BROKERS}" --create \
  --topic berka_orders \
  --partitions 6 --replication-factor 3 || true

kafka-topics.sh --bootstrap-server "${BROKERS}" --create \
  --topic berka_trans \
  --partitions 6 --replication-factor 3 || true

