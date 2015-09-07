/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nventdata.kafkaflink;

import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.BROKER_LIST;
import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.CONFIG_NAME;
import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.JOBNAME;
import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.TOPIC_RANDOM1;
import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.TOPIC_RANDOM2;
import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.TOPIC_RANDOM3;
import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.ZOOKEEPER_ADDRESS;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nventdata.kafkaflink.data.AvroKafkaData;
import com.nventdata.kafkaflink.serde.FlinkKafkaSerDe;
import com.nventdata.kafkaflink.sink.FlinkKafkaTopicWriterSink;
import com.nventdata.kafkaflink.sink.TopicCheckerSink;
import com.nventdata.kafkaflink.util.PropertiesUtil;;

public class FlinkKafkaTopicWriterMain {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaTopicWriterMain.class);

	public static void main(String[] args) throws Exception {
		LOG.debug("Starting FlinkKafkaTopicWriter...");

		Properties config = PropertiesUtil.getPropertiesFromClassPath(CONFIG_NAME);
		
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);
		env.enableCheckpointing(5000);
		
		env.addSource(new KafkaSource<AvroKafkaData>(config.getProperty(ZOOKEEPER_ADDRESS), args[0], new FlinkKafkaSerDe()))
		.flatMap(new FlinkStatLogger()).groupBy(new KeySelector<AvroKafkaData, Integer>(){
					private static final long serialVersionUID = 821378766102201169L;

					public Integer getKey(AvroKafkaData value) throws Exception {
						return value.getRandNum();
					}
					
				})
				.addSink(new FlinkKafkaTopicWriterSink(config.getProperty(BROKER_LIST), new FlinkKafkaSerDe()));
		
		env.addSource(new KafkaSource<AvroKafkaData>(config.getProperty(ZOOKEEPER_ADDRESS), TOPIC_RANDOM1, new FlinkKafkaSerDe())).addSink(new TopicCheckerSink(1));
		env.addSource(new KafkaSource<AvroKafkaData>(config.getProperty(ZOOKEEPER_ADDRESS), TOPIC_RANDOM2, new FlinkKafkaSerDe())).addSink(new TopicCheckerSink(2));
		env.addSource(new KafkaSource<AvroKafkaData>(config.getProperty(ZOOKEEPER_ADDRESS), TOPIC_RANDOM3, new FlinkKafkaSerDe())).addSink(new TopicCheckerSink(3));
		
		System.out.println("Starting job...");
	    
		JobExecutionResult jer = env.execute(JOBNAME);
		
		Long rand1 = jer.getAccumulatorResult("rand1-topic-rec-written");
		Long rand2 = jer.getAccumulatorResult("rand2-topic-rec-written");
		Long rand3 = jer.getAccumulatorResult("rand3-topic-rec-written");
		Long bytesWritten = jer.getAccumulatorResult("total-bytes-written");
		Long recordWritten = jer.getAccumulatorResult("total-record-written");
		System.out.println("Execution time = " + jer.getNetRuntime(TimeUnit.SECONDS)+" sec.");
		System.out.println("Written rand1 = " + rand1+" rand2 = " + rand2 +" rand3 = " + rand3+" totalrec = " + recordWritten+" totalbytes = " + bytesWritten);
	}
}
