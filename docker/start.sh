#!/bin/bash

KAFKA_DIR=/kafka_2.10-0.8.2.1

echo 'Starting Zookeeper'
$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties >zookeeperconsole.log

echo 'Starting Kafka'
$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties >kafkaconsole.log