package com.nventdata.kafkaflink.sink;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.api.config.PartitionerWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.SerializableKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nventdata.kafkaflink.data.AvroKafkaData;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;

public class FlinkKafkaTopicWriterSink extends RichSinkFunction<AvroKafkaData> {
	private static final long serialVersionUID = 8610752883634961942L;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaTopicWriterSink.class);

	private Producer<String, byte[]> producer;
	private Properties userDefinedProperties;
	private String brokerList;
	private SerializationSchema<AvroKafkaData, byte[]> schema;
	private SerializableKafkaPartitioner partitioner;
	private Class<? extends SerializableKafkaPartitioner> partitionerClass = null;
	private LongCounter recRand1 = new LongCounter();
	private LongCounter recRand2 = new LongCounter();
	private LongCounter recRand3 = new LongCounter();
	private LongCounter totalRecordWritten = new LongCounter();
	private LongCounter totalBytesWritten = new LongCounter();

	/**
	 * Creates a KafkaSink for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param brokerList
	 *			Addresses of the brokers
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 */
	public FlinkKafkaTopicWriterSink(String brokerList,
			SerializationSchema<AvroKafkaData, byte[]> serializationSchema) {
		this(brokerList, new Properties(), serializationSchema);
	}

	/**
	 * Creates a KafkaSink for a given topic with custom Producer configuration.
	 * If you use this constructor, the broker should be set with the "metadata.broker.list"
	 * configuration.
	 *
	 * @param brokerList
	 * 		Addresses of the brokers
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param producerConfig
	 * 		Configurations of the Kafka producer
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 */
	public FlinkKafkaTopicWriterSink(String brokerList, Properties producerConfig,
			SerializationSchema<AvroKafkaData, byte[]> serializationSchema) {
		String[] elements = brokerList.split(",");
		for(String broker: elements) {
			NetUtils.ensureCorrectHostnamePort(broker);
		}
		this.brokerList = brokerList;
		this.schema = serializationSchema;
		this.partitionerClass = null;
		this.userDefinedProperties = producerConfig;
	}

	/**
	 * Creates a KafkaSink for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param brokerList
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 * @param partitioner
	 * 		User defined partitioner.
	 */
	public FlinkKafkaTopicWriterSink(String brokerList, String topicId,
			SerializationSchema<AvroKafkaData, byte[]> serializationSchema, SerializableKafkaPartitioner partitioner) {
		this(brokerList, serializationSchema);
		ClosureCleaner.ensureSerializable(partitioner);
		this.partitioner = partitioner;
	}

	public FlinkKafkaTopicWriterSink(String brokerList,
			String topicId,
			SerializationSchema<AvroKafkaData, byte[]> serializationSchema,
			Class<? extends SerializableKafkaPartitioner> partitioner) {
		this(brokerList, serializationSchema);
		this.partitionerClass = partitioner;
	}

	/**
	 * Initializes the connection to Kafka.
	 */
	public void open(Configuration configuration) {

		Properties properties = new Properties();

		properties.put("metadata.broker.list", brokerList);
		properties.put("request.required.acks", "-1");
		properties.put("message.send.max.retries", "10");

		properties.put("serializer.class", DefaultEncoder.class.getCanonicalName());

		// this will not be used as the key will not be serialized
		properties.put("key.serializer.class", DefaultEncoder.class.getCanonicalName());

		for (Map.Entry<Object, Object> propertiesEntry : userDefinedProperties.entrySet()) {
			properties.put(propertiesEntry.getKey(), propertiesEntry.getValue());
		}

		if (partitioner != null) {
			properties.put("partitioner.class", PartitionerWrapper.class.getCanonicalName());
			// java serialization will do the rest.
			properties.put(PartitionerWrapper.SERIALIZED_WRAPPER_NAME, partitioner);
		}
		if (partitionerClass != null) {
			properties.put("partitioner.class", partitionerClass);
		}

		ProducerConfig config = new ProducerConfig(properties);

		try {
			producer = new Producer<String, byte[]>(config);
		} catch (NullPointerException e) {
			throw new RuntimeException("Cannot connect to Kafka broker " + brokerList, e);
		}
		getRuntimeContext().addAccumulator("rand1-topic-rec-written", this.recRand1);
		getRuntimeContext().addAccumulator("rand2-topic-rec-written", this.recRand2);
		getRuntimeContext().addAccumulator("rand3-topic-rec-written", this.recRand3);
		getRuntimeContext().addAccumulator("total-bytes-written", this.totalBytesWritten);
		getRuntimeContext().addAccumulator("total-record-written", this.totalRecordWritten);
	}

	public void invoke(AvroKafkaData next) {
		byte[] serialized = schema.serialize(next);
		producer.send(new KeyedMessage<String, byte[]>("random" + next.getRandNum(), serialized));
		switch(next.getRandNum()){
			case 1: this.recRand1.add(1l);
					break;
			case 2: this.recRand2.add(1l);
					break;
			case 3: this.recRand3.add(1l);
					break;
		}
		this.totalBytesWritten.add(Long.valueOf(serialized.length));
		this.totalRecordWritten.add(1l);
	}

	public void close() {
		if (producer != null) {
			producer.close();
		}
	}

}