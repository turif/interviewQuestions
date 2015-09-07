package com.nventdata.kafkasender;

import static com.nventdata.kafkasender.KafkaSenderConsts.BROKER;
import static com.nventdata.kafkasender.KafkaSenderConsts.BROKER_LIST;
import static com.nventdata.kafkasender.KafkaSenderConsts.CONFIG_NAME;
import static com.nventdata.kafkasender.KafkaSenderConsts.KAFKA_AVRO_CHECKER1;
import static com.nventdata.kafkasender.KafkaSenderConsts.KAFKA_AVRO_CHECKER2;
import static com.nventdata.kafkasender.KafkaSenderConsts.KAFKA_AVRO_CHECKER3;
import static com.nventdata.kafkasender.KafkaSenderConsts.KAFKA_AVRO_GROUPPER;
import static com.nventdata.kafkasender.KafkaSenderConsts.KAFKA_AVRO_SENDER;
import static com.nventdata.kafkasender.KafkaSenderConsts.KAFKA_SPOUT_READER;
import static com.nventdata.kafkasender.KafkaSenderConsts.KAFKA_SPOUT_READER_RANDOM1;
import static com.nventdata.kafkasender.KafkaSenderConsts.KAFKA_SPOUT_READER_RANDOM2;
import static com.nventdata.kafkasender.KafkaSenderConsts.KAFKA_SPOUT_READER_RANDOM3;
import static com.nventdata.kafkasender.KafkaSenderConsts.PRODUCER_TYPE;
import static com.nventdata.kafkasender.KafkaSenderConsts.STORM_ACK_KEY;
import static com.nventdata.kafkasender.KafkaSenderConsts.STORM_METADATA_BROKER_LIST;
import static com.nventdata.kafkasender.KafkaSenderConsts.TOPOLOGY_NAME;

import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nventdata.kafkasender.bolts.KafkaCheckerBolt;
import com.nventdata.kafkasender.bolts.KafkaGrouperBolt;
import com.nventdata.kafkasender.bolts.KafkaOutBolt;
import com.nventdata.kafkasender.util.PropertiesUtil;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.kafka.Broker;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.bolt.selector.KafkaTopicSelector;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.TridentKafkaState;

public class KafkaSenderMain {
	private static final Logger logger = LoggerFactory.getLogger(KafkaSenderMain.class);

	private static Config getConfig(Properties config) {
		Config conf = new Config();
		conf.setNumWorkers(4);
		conf.setDebug(true);
		conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);
		// conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		// conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,40);
		Properties props = new Properties();
		props.put(STORM_METADATA_BROKER_LIST, config.getProperty(BROKER_LIST));
		props.put(STORM_ACK_KEY, "1");
		props.put(PRODUCER_TYPE, config.getProperty(PRODUCER_TYPE));
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);

		return conf;
	}

	public static void main(String[] args)
			throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {

		TopologyBuilder builder = new TopologyBuilder();
		Properties config = PropertiesUtil.getPropertiesFromClassPath(CONFIG_NAME);

		if (args.length >= 1) {
			final String topic = args[0];
			logger.info("Reading from topic = " + topic);

			Broker brokerForPartition0 = new Broker(config.getProperty(BROKER));
			GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
			partitionInfo.addPartition(0, brokerForPartition0);
			StaticHosts hosts = new StaticHosts(partitionInfo);

			SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());

			KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

			builder.setSpout(KAFKA_SPOUT_READER, kafkaSpout, 1);

			builder.setBolt(KAFKA_AVRO_GROUPPER, new KafkaGrouperBolt(), 4).shuffleGrouping(KAFKA_SPOUT_READER);

			builder.setBolt(KAFKA_AVRO_SENDER, new KafkaOutBolt().withTopicSelector(new KafkaTopicSelector() {
				private static final long serialVersionUID = -9189810246209479112L;

				@Override
				public String getTopic(Tuple tuple) {
					Integer rand = (Integer) tuple.getValueByField(KafkaSenderFields.RANDOM);
					return KafkaSenderFields.RANDOM + rand;
				}
			}), 1).fieldsGrouping(KAFKA_AVRO_GROUPPER, new Fields(KafkaSenderFields.RANDOM));

			builder.setSpout(KAFKA_SPOUT_READER_RANDOM1, new KafkaSpout(new SpoutConfig(hosts, "random1", "/random1", UUID.randomUUID().toString())), 1);			
			builder.setSpout(KAFKA_SPOUT_READER_RANDOM2, new KafkaSpout(new SpoutConfig(hosts, "random2", "/random2", UUID.randomUUID().toString())), 1);
			builder.setSpout(KAFKA_SPOUT_READER_RANDOM3, new KafkaSpout(new SpoutConfig(hosts, "random3", "/random3", UUID.randomUUID().toString())), 1);

			builder.setBolt(KAFKA_AVRO_CHECKER1, new KafkaCheckerBolt(1), 4).shuffleGrouping(KAFKA_SPOUT_READER_RANDOM1);
			builder.setBolt(KAFKA_AVRO_CHECKER2, new KafkaCheckerBolt(2), 4).shuffleGrouping(KAFKA_SPOUT_READER_RANDOM2);
			builder.setBolt(KAFKA_AVRO_CHECKER3, new KafkaCheckerBolt(3), 4).shuffleGrouping(KAFKA_SPOUT_READER_RANDOM3);
			
			StormTopology kafkaTopology =  builder.createTopology();
			StormSubmitter.submitTopology(TOPOLOGY_NAME, getConfig(config), kafkaTopology);
		} else {
			throw new IllegalArgumentException("topic name is required");
		}
	}
}