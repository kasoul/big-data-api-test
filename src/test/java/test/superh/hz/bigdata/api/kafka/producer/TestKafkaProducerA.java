package test.superh.hz.bigdata.api.kafka.producer;

import com.superh.hz.bigdata.api.kafka.consumer.KafkaConsumerA;
import com.superh.hz.bigdata.api.kafka.producer.KafkaProducerA;

/** 
 * @Project: big-apple-api-test
 * @File: TestKafkaProducerA.java 
 * @Date: 2015年7月18日 
 * @Author: 黄超（huangchaohz）
 */

/**
 *  @Describe:
 */
public class TestKafkaProducerA {

	/**
	 * @description 
	 * @author 黄超(huangchaohz)
	 * @date 2015年7月18日
	 * @param 
	 * @return 
	 */
	public static void main(String[] args) {
		testKafkaProducerA();
		//testKafakaConsumerA();
	}
	
	public static void testKafkaProducerA(){
		String brokerIp = "192.168.12.48";
		String topic = "test0727";
		KafkaProducerA producer = new KafkaProducerA(brokerIp,topic);
		producer.sendData("123456-5");
	}
	
	public static void testKafakaConsumerA(){
		String zookeeperIp = "192.168.12.48:2181";
		String topic = "test0727";
		KafkaConsumerA consumer = new KafkaConsumerA(zookeeperIp,topic);
		consumer.getData();
	}

}
