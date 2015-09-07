package com.nventdata.kafkaflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.nventdata.kafkaflink.data.AvroKafkaData;

public class FlinkStatLogger implements FlatMapFunction<AvroKafkaData, AvroKafkaData> {
	private static final long serialVersionUID = 7273409286711030543L;
	long received = 0;
	long lastReceived= 0;
	int logRec = 1000;
	long lastLogTime = -1;

	public void flatMap(AvroKafkaData event, Collector<AvroKafkaData> collector) throws Exception {
		received++;
		if (received % logRec == 0) {
			long now = System.currentTimeMillis();
			if(lastLogTime != -1) {
				long timeDiff = now - lastLogTime;
				long elementDiff = received - lastReceived;
				double ex = (1000/(double)timeDiff);
				System.out.println("During the last " + timeDiff + " ms, we received " + elementDiff + " elements. elements/second = " + elementDiff*ex + " Total read = "  + received);
			}
			lastLogTime = now;
			lastReceived = received;
		}
		collector.collect(event);
	}
}
