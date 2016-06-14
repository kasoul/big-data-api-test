package test.superh.hz.bigdata.api.kafka.producer;

import com.superh.hz.bigdata.api.kafka.consumer.KafkaConsumerA;

/** 
 * @Project: big-apple-api-test
 * @File: TestKafkaConsumerA.java 
 * @Date: 2015年7月18日 
 * @Author: 黄超（huangchaohz）
 */

/**
 *  @Describe:
 */
public class TestKafkaConsumerA {

	/**
	 * @description 
	 * @author 黄超(huangchaohz)
	 * @date 2015年7月18日
	 * @param 
	 * @return 
	 */
	public static void main(String[] args) {
		testKafakaConsumerA();
	}
	
	public static void testKafakaConsumerA(){
		String zookeeperIp = "172.30.137.12:2181";
		String topic = "uim-topic-base-nginx-change-detail";
		KafkaConsumerA consumerA = new KafkaConsumerA(zookeeperIp,"grouphc1",topic);
		consumerA.getData();
		try {  
	          Thread.sleep(10000);  
	    } catch (InterruptedException ie) {  
	   
	    }  
		//consumerA.shutdown();  
	}

}
