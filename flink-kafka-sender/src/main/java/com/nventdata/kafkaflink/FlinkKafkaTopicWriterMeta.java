package com.nventdata.kafkaflink;

import org.apache.flink.api.common.ProgramDescription;

public class FlinkKafkaTopicWriterMeta extends FlinkKafkaTopicWriterMain implements ProgramDescription {

	public static void main(String[] args) throws Exception {
		FlinkKafkaTopicWriterMain.main(args);
	}

	public String getDescription() {
		return "Simple Kafka-Sender\n";
	}
}
