package com.nventdata.kafkasender.bolts;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.storm.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nventdata.kafkasender.KafkaSenderFields;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.bolt.selector.KafkaTopicSelector;

public class KafkaOutBolt extends BaseRichBolt {
	private static final long serialVersionUID = -3900809143086865503L;

	private static final Logger logger = LoggerFactory.getLogger(KafkaOutBolt.class);

	private Producer<String, byte[]> producer;
	private OutputCollector collector;
	private KafkaTopicSelector topicSelector;

	private GenericDatumWriter<GenericRecord> avroDataWriter;
	private Schema schema;
	private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();

	MultiCountMetric mainMetrics;

	public KafkaOutBolt withTopicSelector(KafkaTopicSelector selector) {
		this.topicSelector = selector;
		return this;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		logger.debug("Preparing KafkaSpout.");
		if (topicSelector == null) {
			this.topicSelector = new DefaultTopicSelector((String) stormConf.get("topic"));
		}

		Map configMap = (Map) stormConf.get("kafka.broker.properties");
		Properties properties = new Properties();
		properties.putAll(configMap);
		ProducerConfig config = new ProducerConfig(properties);
		producer = new Producer<String, byte[]>(config);
		this.collector = collector;

		Schema.Parser parser = new Schema.Parser();
		try {
			schema = parser.parse(getClass().getResourceAsStream("/avro_schema.json"));
			avroDataWriter = new GenericDatumWriter<GenericRecord>();
			avroDataWriter.setSchema(schema);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		mainMetrics = (MultiCountMetric)context.getRegisteredMetricByName("mainMetrics");
		if (mainMetrics == null){
			mainMetrics = new MultiCountMetric();
			context.registerMetric("mainMetrics", mainMetrics, 1);
		}
	}

	private void updateMetrics(long ms,long processedbytes) {
		mainMetrics.scope("execms_kafkaout").incrBy(ms);
		mainMetrics.scope("execnum_kafkaout").incr();
		mainMetrics.scope("execbytes_kafkaout").incrBy(processedbytes);
	}	
	
	@Override
	public void execute(Tuple input) {
		if (TupleUtils.isTick(input)) {
			collector.ack(input);
			return;
		}
		
		
		String topic = null;
		try {
			long start = System.currentTimeMillis();
			topic = topicSelector.getTopic(input);
			Integer id = (Integer) input.getValueByField("id");
			Integer rand = (Integer) input.getValueByField("random");
			String data = (String) input.getValueByField("data");

			GenericRecord rec = new GenericData.Record(schema);
			rec.put("id", id);
			rec.put("random", rand);
			rec.put("data", data);

			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			BinaryEncoder binaryEncoder = avroEncoderFactory.binaryEncoder(stream, null);

			avroDataWriter.write(rec, binaryEncoder);
			binaryEncoder.flush();
			IOUtils.closeQuietly(stream);
			logger.debug("Sending message to Kafka topic = " + topic + " message...");
			byte[]barray = stream.toByteArray();
			producer.send(new KeyedMessage<String, byte[]>(topic, barray));
			//updateMetrics(rec);
			long end = System.currentTimeMillis();
			collector.ack(input);
			updateMetrics(end - start,barray.length);
		} catch (Exception ex) {
			collector.reportError(ex);
			collector.fail(input);
		}
	}

	/*
	private void updateMetrics(GenericRecord rec) {
		CountMetric met = mainMetrics.scope("random" + rec.get(KafkaSenderFields.RANDOM));
		met.incrBy(1);
	}*/

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
