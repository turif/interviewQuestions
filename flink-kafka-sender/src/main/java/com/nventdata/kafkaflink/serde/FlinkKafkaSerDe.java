package com.nventdata.kafkaflink.serde;

import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.FIELD_DATA;
import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.FIELD_ID;
import static com.nventdata.kafkaflink.FlinkKafkaTopicConsts.FIELD_RANDOM;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.util.IOUtils;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nventdata.kafkaflink.FlinkKafkaTopicWriterMain;
import com.nventdata.kafkaflink.data.AvroKafkaData;


public class FlinkKafkaSerDe implements DeserializationSchema<AvroKafkaData>,SerializationSchema<AvroKafkaData, byte[]>{
	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaTopicWriterMain.class);

	private static final long serialVersionUID = -8314881700393464119L;
    private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();    

	public FlinkKafkaSerDe(){
	}

	private Schema getSchema(){
        return new Schema.Parser().parse("{\"name\":\"kafkatest\",\"namespace\":\"test\",\"type\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"random\",\"type\":\"int\"},{\"name\":\"data\",\"type\":\"string\"}]}");
	}
	
	public TypeInformation<AvroKafkaData> getProducedType() {
		return TypeExtractor.getForClass(AvroKafkaData.class);
	}

	public AvroKafkaData deserialize(byte[] message) {
		AvroKafkaData data = null;
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(getSchema());
            Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
            GenericRecord result = reader.read(null, decoder);
            data = new AvroKafkaData((Integer)result.get(FIELD_ID),(Integer)result.get(FIELD_RANDOM),String.valueOf(result.get(FIELD_DATA)));
            LOG.debug("Read kafka data: " + data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
		return data;
	}

	public boolean isEndOfStream(AvroKafkaData nextElement) {
		return false;
	}

	public byte[] serialize(AvroKafkaData element) {
		LOG.debug("Serializing element = " + element);
        byte[] data = null;
        try {
        	Schema schema = getSchema();
    		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);    		
    		
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            
    	    BinaryEncoder binaryEncoder = avroEncoderFactory.binaryEncoder(stream, null);
            
    	    GenericRecord rec = new GenericData.Record(schema);
    	    rec.put(FIELD_ID, element.getId());
    	    rec.put(FIELD_RANDOM, element.getRandNum());
    	    rec.put(FIELD_DATA, element.getData());

            writer.write(rec, binaryEncoder);
            binaryEncoder.flush();
            IOUtils.closeStream(stream);
            
            data = stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
		return data;
	}

}
