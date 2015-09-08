# Directory structure
docker
 * contains the docker file 

flink-kafka-sender
  * flink version of kafka sender

storm-kafka-sender
  * storm version of kafka sender

# Starting the storm cluster

1. $STORM_DIR/bin/storm nimbus &

2. $STORM_DIR/bin/storm supervisor &

3. $STORM_DIR/bin/storm ui &

# Building Storm Kafka Sender

