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
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.TupleUtils;
import kafka.message.Message;

public class KafkaCheckerBolt extends BaseRichBolt {
	private static final long serialVersionUID = 7494809736412431552L;
	private static final Logger logger = LoggerFactory.getLogger(KafkaSenderMain.class);
	private OutputCollector _collector;
	private Schema _schema;
	private DatumReader<GenericRecord> reader;
	MultiCountMetric mainMetrics;
	private Integer expectedRand;
	
	public KafkaCheckerBolt(Integer exprand) {
		this.expectedRand = exprand;
	}
	
	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		logger.debug("Executing KafkaCheckerBolt.Prepare...");
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
		MultiCountMetric rm = (MultiCountMetric)context.getRegisteredMetricByName("mainMetrics");
		if (rm == null){
			mainMetrics = new MultiCountMetric();
			context.registerMetric("mainMetrics", mainMetrics, 1);
		}
	}

	private void updateMetrics(long ms,long processedbytes) {
		mainMetrics.scope("execms_checker").incrBy(ms);
		mainMetrics.scope("execnum_checker").incr();
		mainMetrics.scope("execbytes_checker").incrBy(processedbytes);
	}
		
	@Override
	public void execute(Tuple tuple) {
		if (TupleUtils.isTick(tuple)) {
			_collector.ack(tuple);
			return;
		}
		logger.info("Executing KafkaCheckerBolt...");

		long start = System.currentTimeMillis();
		Message message = new Message((byte[]) ((TupleImpl) tuple).get("bytes"));

		ByteBuffer bb = message.payload();

		byte[] b = new byte[bb.remaining()];
		bb.get(b, 0, b.length);

		try {
			Decoder decoder = DecoderFactory.get().binaryDecoder(b, null);
			GenericRecord result = reader.read(null, decoder);
			Integer readVal = (Integer) result.get(KafkaSenderFields.RANDOM);
			if (readVal.intValue() != expectedRand.intValue()){
				_collector.fail(tuple);
			}else{
				_collector.ack(tuple);
			}
		} catch (IOException e) {
			_collector.fail(tuple);
			throw new RuntimeException(e);
		}
		long end = System.currentTimeMillis();
		updateMetrics(end - start,b.length);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
}