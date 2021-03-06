package test.superh.hz.bigdata.api.kafka.producer;

import com.superh.hz.bigdata.api.kafka.consumer.KafkaConsumerA;
import com.superh.hz.bigdata.api.kafka.producer.KafkaProducerA;

import net.sf.json.JSONObject;

/**
 * 2015-7-18
 */
public class TestKafkaProducerA {

	public static void main(String[] args) {
		testKafkaProducerA();
		//testKafakaConsumerA();
	}
	
	public static void testKafkaProducerA(){
		String brokerIp = "node3";
		String topic = "hctest";
		KafkaProducerA producer = new KafkaProducerA(brokerIp,topic);
		JSONObject jsonIn = new JSONObject();
		jsonIn.element("name", "Kevin");
		jsonIn.element("age", "12");
		jsonIn.element("address", "Hubei middle road 7#");
		jsonIn.element("phone", "13566666666");
		//JSONObject jsonOut = JSONObject.fromObject(jsonIn.toString());
		//System.out.println(jsonOut.getString("name"));
		//System.out.println(jsonOut.getString("address"));
		//System.out.println(jsonOut.getString("age"));
		producer.sendData(jsonIn.toString());
	}
	
	public static void testKafakaConsumerA(){
		String zookeeperIp = "192.168.0.1:2181";
		String topic = "test0727";
		KafkaConsumerA consumer = new KafkaConsumerA(zookeeperIp,topic);
		
		consumer.getData();
	}

}
