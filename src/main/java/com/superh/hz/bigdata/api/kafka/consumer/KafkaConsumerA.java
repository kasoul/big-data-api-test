package com.superh.hz.bigdata.api.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
/*import kafka.consumer.ConsumerIterator;*/
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  消费者组
 *  2016-2-15
 */
public class KafkaConsumerA {
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerA.class);

	private ConsumerConnector consumer;
	private String topic = null;
	private String defaultGroupId = "group0";
	private int numConsumers = 1;
	
	private ExecutorService executor;
	
	public KafkaConsumerA(String zookeeper, String groupId, String topic,int numConsumers) {
	    consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,groupId));
	    this.topic = topic;
	    this.numConsumers = numConsumers;
	}
	
	public KafkaConsumerA(String zookeeper, String groupId, String topic) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,groupId));
        this.topic = topic;
	}
	
	public KafkaConsumerA(String zookeeper, String topic) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,defaultGroupId));
        this.topic = topic;
	}

	
	private ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		 Properties props = new Properties();
	     props.put("zookeeper.connect", zookeeper);
	     props.put("group.id", groupId);
		 props.put("conusmer.id", "hc_conusmer1");
		 props.put("client.id", "hc_conusmer_client1");

	     props.put("auto.offset.reset", "largest");
	     props.put("zookeeper.session.timeout.ms", "20000");
	     props.put("zookeeper.connection.timeout.ms", "20000");
	     //props.put("zookeeper.sync.time.ms", "200");
	     props.put("auto.commit.interval.ms", "3000");
	 
	     return new ConsumerConfig(props);
	}
	
	public void getData(){
		//一个topic有多个消费者
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(numConsumers));
		
		//consumerMap的key是topic，value是多个消费者返回的list，按消费者生成的顺序排列
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		logger.info(streams.size()+"");
		 
		executor = Executors.newFixedThreadPool(numConsumers);
		 
	    int threadNumber = 0;
	    
	    //获取一个topic每个消费者的数据
	    for (final KafkaStream<byte[], byte[]> stream : streams) {
	         executor.submit(new ConsumerAThread(stream, threadNumber)); // 启动consumer thread
	         threadNumber++;
	    }
	    
		/*for(KafkaStream<byte[], byte[]> ks : streams){
			
			 ConsumerIterator<byte[], byte[]> it = ks.iterator();
		     while (it.hasNext()){
		    	 logger.info("Thread " + ks + ": "
		                    + new String(it.next().message()));
		     }
		    	 logger.info("Shutting down Thread: " + ks);
		}*/
		
	}
	
	public void shutdown() {
	        if (consumer != null) consumer.shutdown();
	        if (executor != null) executor.shutdown();
	}
}
