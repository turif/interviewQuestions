# Directory structure
docker
 * contains the docker file 

flink-kafka-sender
  * flink version of kafka sender

storm-kafka-sender
  * storm version of kafka sender

#Building the Docker from dockerfile where the docker file in the current directory
* sudo docker build -t kafkatest .

# Running the Docker Container
It starts zookeeper & kafka server
* docker run -d -t -p 9092:9092 -p 2181:2181 kafkatest

# Starting the storm cluster

1. $STORM_DIR/bin/storm nimbus &

2. $STORM_DIR/bin/storm supervisor &

3. $STORM_DIR/bin/storm ui &

# Building and Running Storm Kafka Sender

1.  dependencies should be copied to the $STORM_HOME/extlib folder which contains redistributable dependencies.
3rd party dependencies

avro-1.7.7.jar
curator-client-2.5.0.jar
curator-framework-2.5.0.jar
guava-16.0.1.jar
jackson-core-asl-1.9.13.jar
jackson-dataformat-smile-2.3.1.jar
jackson-mapper-asl-1.9.13.jar
jopt-simple-3.2.jar
json-simple-1.1.jar
kafka_2.10-0.8.2.1.jar
kafka-clients-0.8.2.1.jar
metrics-core-2.2.0.jar
scala-library-2.10.4.jar
storm-kafka-0.10.0-beta1.jar
zkclient-0.3.jar
zookeeper-3.4.6.jar

2.  building the lightweight jar. The lightweight jar does not contain 3rd party dependencies. Should be provided prior in the extlib folder to make development faster. In production building a FAT jar is more advisable as It contains all the required dependencies. In development building a lightweight jar speeds up development thus packaging is more faster.

-->mvn clean package

3. running the compiled binary
The first parameter of KafkaSenderMain is the name of topic which should be read by Storm.

-->storm jar storm-kafka-sender-1.0.jar com.nventdata.kafkasender.KafkaSenderMain neverwinter

# Starting the flink Cluster

1. #$FLINK_DIR/bin/start-local.sh &

# Building and Running Flink Kafka Sender

1.  building the FAT jar. The FAT jar contains all 3rd party dependencies required by the Flink Kafka Sender.

-->mvn clean package

2. running the compiled binary
The first parameter of KafkaSenderMain is the name of topic which should be read by Flink.

flink run flink-kafka-sender-1.0.jar neverwinter
