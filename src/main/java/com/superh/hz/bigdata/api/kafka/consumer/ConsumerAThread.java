package com.superh.hz.bigdata.api.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/** 
 * @Project: big-apple-api-test
 * @File: ConsumerAThread.java 
 * @Date: 2016年2月15日 
 * @Author: 黄超（huangchaohz）
 */

/**
 *  @Describe:单个消费者线程
 */
public class ConsumerAThread implements Runnable{
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerA.class);
	
	private KafkaStream<byte[], byte[]> m_stream;  
    private int m_threadNumber;  
   
    public ConsumerAThread(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber) {  
        m_threadNumber = a_threadNumber;  
        m_stream = a_stream;  
    }  
   
    public void run() {  
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();  
        while (it.hasNext()){
        	
        	logger.info("Thread " + m_threadNumber + ": " + new String(it.next().message()));  
        }
        logger.error("Shutting down Thread: " + m_threadNumber);
        	
    }
}
