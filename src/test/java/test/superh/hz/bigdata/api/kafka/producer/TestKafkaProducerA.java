package test.superh.hz.bigdata.api.kafka.producer;

import com.superh.hz.bigdata.api.kafka.consumer.KafkaConsumerA;
import com.superh.hz.bigdata.api.kafka.producer.KafkaProducerA;

/**
 * 2015-7-18
 */
public class TestKafkaProducerA {

	public static void main(String[] args) {
		testKafkaProducerA();
		//testKafakaConsumerA();
	}
	
	public static void testKafkaProducerA(){
		String brokerIp = "192.168.0.0";
		String topic = "test0727";
		KafkaProducerA producer = new KafkaProducerA(brokerIp,topic);
		producer.sendData("123456-5");
	}
	
	public static void testKafakaConsumerA(){
		String zookeeperIp = "192.168.0.1:2181";
		String topic = "test0727";
		KafkaConsumerA consumer = new KafkaConsumerA(zookeeperIp,topic);
		consumer.getData();
	}

}
