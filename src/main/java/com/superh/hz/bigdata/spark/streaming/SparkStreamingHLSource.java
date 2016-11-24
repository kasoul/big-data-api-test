package com.superh.hz.bigdata.spark.streaming;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * @Describe:高级数据源的spark-streaming应用程序
 */
public class SparkStreamingHLSource {

	public static void main(String[] args) {
		buildKafkaStream();
	}

	private static void buildKafkaStream() {
		SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
		conf.setMaster("local[*]");

		conf.set("spark.streaming.blockInterval", "1000");
		conf.set("spark.streaming.receiver.maxRate", "100");
		conf.set("spark.streaming.receiver.writeAheadLog.enable","true");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("zookeeper.connect", "node1,node2,node3");
		kafkaParams.put("group.id", "test_consumer_group_spark_streaming");
		kafkaParams.put("auto.offset.reset", "largest");
		//kafkaParams.put("auto.offset.reset", "largest");
		//String zkQuorum = "node1,node2,node3";
		//String consumerGroupId = "test_consumer_group_spark_streaming";
		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put("hctest",5);


		JavaPairReceiverInputDStream<String, byte[]> kafkaStream = 
			     KafkaUtils.<String,byte[],StringDecoder,DefaultDecoder>createStream(
			    		 jssc,
			    		 String.class,
			    		 byte[].class,
			    		 StringDecoder.class,
			    		 DefaultDecoder.class,
			    		 kafkaParams,topicMap,
			    		 StorageLevel.MEMORY_AND_DISK_SER_2());
				//KafkaUtils.createStream(jssc,zkQuorum,consumerGroupId,topicMap);
		JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, byte[]>, String>() {

			@Override
		      public String call(Tuple2<String, byte[]> tuple2) {

		    	return new String(tuple2._2());
		    	//return String.valueOf(tuple2._2().length);

		      }
		});
		
		lines.print(2);
		lines.count().print();
		// wordCounts.saveAsHadoopFiles(prefix, suffix);
		jssc.checkpoint("hdfs://mycluster/user/hadoop/hc/checkpoint");
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
	}
}
