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

# Building and Running Storm Kafka Sender

1.  dependencies should be copied to the $STORM_HOME/extlib folder which contains redistributable dependencies.

2.  building the lightweight jar. The lightweight jar does not contain 3rd party dependencies. Should be provided prior in the extlib folder.

-->mvn clean package

3. running the compiled binary
The first parameter is the name of topic which should be read by storm.

-->storm jar storm-kafka-sender-1.0.jar com.nventdata.kafkasender.KafkaSenderMain neverwinter


