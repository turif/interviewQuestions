package com.nventdata.kafkasender.bolts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nventdata.kafkasender.KafkaSenderFields;
import com.nventdata.kafkasender.KafkaSenderMain;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleUtils;
import kafka.message.Message;

public class KafkaGrouperBolt extends BaseRichBolt {
	private static final Logger logger = LoggerFactory.getLogger(KafkaSenderMain.class);
	private static final long serialVersionUID = 4005923503453467468L;
	private OutputCollector _collector;
	private Schema _schema;
	private DatumReader<GenericRecord> reader;
	MultiCountMetric mainMetrics;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		logger.debug("Executing KafkaGrouperBolt.Prepare...");
		_collector = outputCollector;
		try {
			Schema.Parser parser = new Schema.Parser();
			_schema = parser.parse(getClass().getResourceAsStream("/avro_schema.json"));
			reader = new GenericDatumReader<GenericRecord>(_schema);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		initMetrics(topologyContext);		
	}

	void initMetrics(TopologyContext context) {
		mainMetrics = new MultiCountMetric();
		context.registerMetric("mainMetrics", mainMetrics, 1);
	}

	private void updateMetrics(long ms,long processedbytes) {
		mainMetrics.scope("execms_groupper").incrBy(ms);
		mainMetrics.scope("execnum_groupper").incr();
		mainMetrics.scope("execbytes_groupper").incrBy(processedbytes);
	}
		
	@Override
	public void execute(Tuple tuple) {
		if (TupleUtils.isTick(tuple)) {
			_collector.ack(tuple);
			return;
		}
		logger.info("Executing KafkaGrouperBolt...");

		long start = System.currentTimeMillis();
		Message message = new Message((byte[]) ((TupleImpl) tuple).get("bytes"));

		ByteBuffer bb = message.payload();

		byte[] b = new byte[bb.remaining()];
		bb.get(b, 0, b.length);

		try {
			Decoder decoder = DecoderFactory.get().binaryDecoder(b, null);
			GenericRecord result = reader.read(null, decoder);
			_collector.emit(new Values((Integer) result.get(KafkaSenderFields.ID),
					(Integer) result.get(KafkaSenderFields.RANDOM),
					String.valueOf(result.get(KafkaSenderFields.DATA))));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		long end = System.currentTimeMillis();
		_collector.ack(tuple);
		updateMetrics(end - start,b.length);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer
				.declare(new Fields(KafkaSenderFields.ID, KafkaSenderFields.RANDOM, KafkaSenderFields.DATA));
	}
}