package com.nventdata.kafkasender;

public class KafkaSenderConsts {
	public static final String CONFIG_NAME = "config.properties";
	public static final String ZOOKEEPER_ADDRESS = "zookeeper.address";
	public static final String BROKER = "broker";
	public static final String BROKER_LIST = "broker.list";
	public static final String PRODUCER_TYPE = "producer.type";
	public static final String STORM_ACK_KEY = "request.required.acks";
	public static final String STORM_METADATA_BROKER_LIST = "metadata.broker.list";
	
	public static final String TOPOLOGY_NAME = "Kafka-Sender-Topology";
	public static final String KAFKA_SPOUT_READER = "kafka-avro-reader";
	public static final String KAFKA_AVRO_GROUPPER = "kafka-avro-groupper";
	public static final String KAFKA_AVRO_SENDER = "kafka-avro-sender";
	public static final String KAFKA_SPOUT_READER_RANDOM1 = "kafka-avro-reader-random1";
	public static final String KAFKA_SPOUT_READER_RANDOM2 = "kafka-avro-reader-random2";
	public static final String KAFKA_SPOUT_READER_RANDOM3 = "kafka-avro-reader-random3";
	public static final String KAFKA_AVRO_CHECKER1 = "kafka-avro-checker1";
	public static final String KAFKA_AVRO_CHECKER2 = "kafka-avro-checker2";
	public static final String KAFKA_AVRO_CHECKER3 = "kafka-avro-checker3";

}
