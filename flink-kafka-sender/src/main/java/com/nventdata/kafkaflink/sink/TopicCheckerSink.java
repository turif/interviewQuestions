package com.nventdata.kafkaflink.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.nventdata.kafkaflink.data.AvroKafkaData;

public class TopicCheckerSink extends RichSinkFunction<AvroKafkaData> {
	private static final long serialVersionUID = -7094507865191426621L;
	private Integer value;
	
	public TopicCheckerSink(Integer valueToCheck){
		value = valueToCheck;
	}
	
	@Override
	public void invoke(AvroKafkaData data) throws Exception {
		if (data.getRandNum().intValue() != value.intValue()){
			throw new Exception("Wrong message random value! Required = " + value + " got = " + data.getRandNum());
		}
	}

}
