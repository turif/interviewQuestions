import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nventdata.kafkaflink.data.AvroKafkaData;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;	

public class KafkaTopicTest {
    private static final int SO_TIMEOUT = 100000; // socket timeout
    private static final int BUFFER_SIZE = 64 * 1024; // maximum socket receive buffer in bytes
    private static final int FETCH_SIZE = 100000; // maximum bytes to fetch from topic
    private static final int PARTITION = 0;
	private static Schema schema = null;
	private static DatumReader<GenericRecord> reader = null;
	private static final String host = "localhost";
    private static final int port = 9092;
    
    
	@BeforeClass
	public static void setUpClass() throws Exception {
		System.out.println("Initializing Unit Test!");
        Schema.Parser parser = new Schema.Parser();
        try {
        	InputStream is = KafkaTopicTest.class.getResourceAsStream("/avro_schema.json");
        	if (is != null){
        		schema = parser.parse(is);
        	}
        } catch (IOException e) {
        	e.printStackTrace();
            throw new RuntimeException(e);
        }
        reader = new GenericDatumReader<GenericRecord>(schema);
        
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
		System.out.println("Cleaning Up Unit Test!");
	}

	public AvroKafkaData deserialize(byte[] message) {
		AvroKafkaData data = null;
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
            GenericRecord result = reader.read(null, decoder);
            data = new AvroKafkaData((Integer)result.get("id"),(Integer)result.get("random"),String.valueOf(result.get("data")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
		return data;
	}
	
	
	 private long checkMessages(ByteBufferMessageSet messageSet,String topic, int reqVal) throws Exception {
		long nextOffset = - 1;
	    for(MessageAndOffset messageAndOffset: messageSet) {
	      ByteBuffer payload = messageAndOffset.message().payload();
	      nextOffset = messageAndOffset.nextOffset();
	      byte[] bytes = new byte[payload.limit()];
	      payload.get(bytes);
	      AvroKafkaData d = deserialize(bytes);
	      System.out.println("offset:" + messageAndOffset.offset() + " message:" + d);
	      if (d.getRandNum().intValue() != reqVal){
	    	  throw new Exception("Wrong wrong random value for topic = " + topic + " required = " + reqVal + " actual = " + d.getRandNum());
	      }
	    }
	    return nextOffset;
	 }	
	
	@Test
	public void test1() throws Exception{
		testTopic("flinkrandom1", 1);
	}

	@Test
	public void test2() throws Exception{
		testTopic("flinkrandom2", 2);
	}

	@Test
	public void test3() throws Exception {
		testTopic("flinkrandom3", 3);
	}
	
	public void testTopic(String topic,int reqRandomVal) throws Exception {
        long randID = new Random().nextLong();
        SimpleConsumer consumer = new SimpleConsumer(host, port, SO_TIMEOUT, BUFFER_SIZE, "clientId" + randID);
        long lastoffset = 0l;
        while (true){
	        FetchRequest req = new FetchRequestBuilder().clientId("clientId1" + randID).addFetch(topic, 0, lastoffset, FETCH_SIZE).build();
	        kafka.javaapi.FetchResponse fetchResponse = consumer.fetch(req);
	        ByteBufferMessageSet bbms = fetchResponse.messageSet(topic, PARTITION);
	        long offset = checkMessages(bbms,topic, reqRandomVal);
	        if (offset < lastoffset){
	        	break;
	        }else{
	        	lastoffset = offset;
	        }
        }
        if (consumer != null){ 
        	consumer.close();
        }
	    System.out.println("Read Finished!");
    }
}