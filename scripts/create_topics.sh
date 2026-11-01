#!/usr/bin/env bash
# Create Kafka topics used by the streaming pipeline.
set -euo pipefail
KT="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
$KT --create --if-not-exists --topic sales-transactions --partitions 3 --replication-factor 1
$KT --create --if-not-exists --topic restock-alerts     --partitions 1 --replication-factor 1
$KT --list
$KT --describe --topic sales-transactions
