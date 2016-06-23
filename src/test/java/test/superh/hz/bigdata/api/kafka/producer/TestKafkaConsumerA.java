package test.superh.hz.bigdata.api.kafka.producer;

import com.superh.hz.bigdata.api.kafka.consumer.KafkaConsumerA;

/**
 * 2015-7-18
 */
public class TestKafkaConsumerA {

	private static String tableTopic = "test_topic_1";
	
	public static void main(String[] args) {
		testKafakaConsumerA();
	}
	
	public static void testKafakaConsumerA(){
		String zookeeperIp = "127.0.0.1:2181";
		String topic = tableTopic;
		KafkaConsumerA consumerA = new KafkaConsumerA(zookeeperIp,"grouphc1",topic);
		consumerA.getData();
		try {  
	          Thread.sleep(10000);  
	    } catch (InterruptedException ie) {  
	   
	    }  
		//consumerA.shutdown();  
	}

}
