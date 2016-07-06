package com.superh.hz.bigdata.api.kafka.producer;

public class KafkaConstants {
	// kafka 配置
	public static final String KAFKA_ACKS = "1";
	public static final String KAFKA_TYPE = "async";
	public static final String KAFKA_BATCH_NUM = "1000";
	public static final String KAFKA_QUEUE_BUFFERINF_MAX_MS = "100";
	public static final String KAFKA_SERIALIZER = "kafka.serializer.StringEncoder";
	public static final String KAFKA_PARTITIONER = "com.superh.hz.bigdata.api.kafka.producer.RunPartition";
	public static final String KAFKA_PORT = "9092";
}
