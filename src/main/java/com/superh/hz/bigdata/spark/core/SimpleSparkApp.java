package com.superh.hz.bigdata.spark.core;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @Describe:简单的spark应用程序
 */
public class SimpleSparkApp {
	public static void main(String[] args) {
		//testSparkContainChar(args[0]);
		testSparkContainChar2();

	}

	public static void testPath() {
		String classpath = null;
		try {
			classpath = SimpleSparkApp.class.getClassLoader().getResource("").toURI().getPath();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		String classpath2 = SimpleSparkApp.class.getClassLoader().getResource("").getPath();
		String classpath3 = null;
		try {
			classpath3 = URLDecoder.decode(classpath2, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		System.out.println(classpath);
		System.out.println(classpath2);
		System.out.println(classpath3);
	}

	//统计包含指定字符的行数
	public static void testSparkContainChar(String path) {
		String classpath;
		try {
			classpath = SimpleSparkApp.class.getClassLoader().getResource("").toURI().getPath();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		classpath = "C:\\Users\\Administrator\\Desktop\\testdata\\";
		String logFile = classpath + "test-spark.txt"; // Should be some file on
														// your system
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		//conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(path).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}
	
	//使用lambda表达式
	public static void testSparkContainChar2() {
			String classpath = "";
			try {
				classpath = SimpleSparkApp.class.getClassLoader().getResource("").toURI().getPath();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//classpath = "C:\\Users\\Administrator\\Desktop\\testdata\\";
			String logFile = "file://" + classpath + "test-spark.txt"; // Should be some file on
															// your system
			SparkConf conf = new SparkConf().setAppName("Simple Application");
			conf.setMaster("local[*]");
			JavaSparkContext sc = new JavaSparkContext(conf);
			JavaRDD<String> logData = sc.textFile(logFile).cache();
			final Accumulator<Integer> accum = sc.accumulator(10);
			
			logData.foreach(s -> accum.add(s.length()));
			//long totalLenth = rdd2.reduce((a,b) -> a + b);
			System.out.println("accum value: " + accum.value());
			
			/*JavaRDD<Integer> rdd2 = logData.map(s -> s.length());
			long totalLenth = rdd2.reduce((a,b) -> a + b);
			System.out.println("total length: " + totalLenth);*/
			
			
			/*long numAs = logData.filter(s -> s.contains("a")).count();
			long numBs = logData.filter(s -> s.contains("b")).count();
			System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);*/
			
	}

	//统计每行字符串长度
	public static void testSparkSumLineLength() {
		String classpath = "";
		try {
			classpath = SimpleSparkApp.class.getClassLoader().getResource("").toURI().getPath();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		classpath = "C:\\Users\\Administrator\\Desktop\\testdata\\";
		String logFile = classpath + "test-spark.txt"; // Should be some file on
														// your system
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		//conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(logFile).cache();
		JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
			public Integer call(String s) {
				return s.length();
			}
		});
		
		/*int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});*/
		List<Integer> result = lineLengths.collect();
		for(int i=0; i<result.size(); i++){
			System.out.println("第" + i +"行长度:" + result.get(i));
		}
		//System.out.println(totalLength);
	}
}
