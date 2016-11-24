package com.superh.hz.bigdata.spark.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * @Describe:简单的spark-streaming应用程序
 */
public class JavaNetworkWordCount {

    public static void main(String[] args) {

        buildSocketStream();

    }

    public static void buildSocketStream() {
        SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
        conf.setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop1", 9999,
                StorageLevels.MEMORY_AND_DISK_SER_2);// nc -lk 9999

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Arrays.asList(x.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        // Print the first ten elements of each RDD generated in this DStream to
        // the console
        wordCounts.print();
        // wordCounts.saveAsHadoopFiles(prefix, suffix);
        jssc.start(); // Start the computation
        jssc.awaitTermination(); // Wait for the computation to terminate
    }

    public static void buildSocketStreamWithUpdateState() {
        SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
        conf.setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop1", 9999,StorageLevels.MEMORY_AND_DISK_SER_2);// nc
        // -lk
        // 9999,create
        // DStreams

        jssc.checkpoint("hdfs://node1:8020/user/hadoop/hc/checkpoint");

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Arrays.asList(x.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                Integer newSum = state.or(0) + values.size();
                return Optional.of(newSum);
            }
        };

        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(updateFunction);

        // Print the first ten elements of each RDD generated in this DStream to
        // the console
        wordCounts.print();

        jssc.start(); // Start the computation
        jssc.awaitTermination(); // Wait for the computation to terminate
    }

    public static void buildRecoverableSocketStream() {


       String chechpointdirectory = "hdfs://node1:8020/user/hadoop/hc/checkpoint";

        JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
            @Override
            public JavaStreamingContext create() {
                return createContext(chechpointdirectory);
            }
        };


        JavaStreamingContext context = JavaStreamingContext.getOrCreate(chechpointdirectory,
                contextFactory);
        context.start(); // Start the computation
        context.awaitTermination(); // Wait for the computation to terminate
    }

    public static JavaStreamingContext createContext(String chechpointdirectory) {
        SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
        conf.setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10)); // new
        // context
        JavaDStream<String> lines = jssc.socketTextStream("hadoop1", 9999,StorageLevels.MEMORY_AND_DISK_SER_2);// nc
        // -lk
        // 9999,create
        // DStreams

        FlatMapFunction<String, String> flatMapFunction = new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Arrays.asList(x.split(" "));
            }
        };
        JavaDStream<String> words = lines.flatMap(flatMapFunction);

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                Integer newSum = state.or(0) + values.size();
                return Optional.of(newSum);
            }
        };

        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(updateFunction);
        wordCounts.print();
        // wordCounts.checkpoint(Durations.seconds(10));
        jssc.checkpoint("hdfs://node1:8020/user/hadoop/hc/checkpoint"); // set
        // checkpoint
        // directory
        // wordCounts.checkpoint(Durations.seconds(10));

        return jssc;
    }

}
