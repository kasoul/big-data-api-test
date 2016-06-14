package com.superh.hz.bigdata.api.kafka.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *  生产者
 *  2016-2-15
 */
public class KafkaProducerA {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerA.class);

	private Producer<String, String> producer = null;

	private String brokerIp = null;
	private String topic = null;

	public KafkaProducerA(String brokerIp, String topic) {
		this.brokerIp = brokerIp;
		this.topic = topic;
		this.producer = new Producer<String, String>(getProducerConfig());
	}

	public ProducerConfig getProducerConfig() {

		Properties props = new Properties();

		props.put("serializer.class", KafkaConstants.KAFKA_SERIALIZER);
		
		//采用默认分区
		//props.put("partitioner.class",KafkaConstants.KAFKA_PARTITIONER);
		
		//ack机制启动
		props.put("request.required.acks", KafkaConstants.KAFKA_ACKS);
		// 异步发送
		props.put("producer.type", KafkaConstants.KAFKA_TYPE);
		// 每次发送多少条
		props.put("batch.num.messages", KafkaConstants.KAFKA_BATCH_NUM);
		//缓存500ms后发送
		props.put("queue.buffering.max.ms", KafkaConstants.KAFKA_QUEUE_BUFFERINF_MAX_MS);
		
		// #broker用于接收producer消息的hostname以及监听端口
		props.put("metadata.broker.list", this.brokerIp + ":" + KafkaConstants.KAFKA_PORT);
		ProducerConfig config = new ProducerConfig(props);

		return config;
	}

	public void sendData(String input) {

		// 当topic不存在的时候，会创建topic,，数据的分发策略为用户的imsi号
		try {
			if ( input != null && !"".equals(input) ) {
				producer.send(new KeyedMessage<String, String>(this.topic, "key", input));
				Thread.sleep(1000);
				
				logger.info("成功发送一条记录：{ " + input + " } ");
			} else {
				throw new RuntimeException("数据不符合要求！");
			}
		} catch (Exception e) {
			logger.info("发送数据失败！ 原因：" + e.getMessage());
		}
		
		producer.close();
	}
}
