package com.superh.hz.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author huangchao
 * 简单的spark-sql应用程序
 */
public class SimpleSparkSQLApp {
	public static void main(String[] args) {
		
		String sqlMaterial = "file:///C:\\Users\\Administrator\\Desktop\\testdata\\test-spark-sql.txt";
		testSparkSql(sqlMaterial);
		
	}

	//SQLContext
	public static void testSparkSql(String path) {
		
		SparkConf sparkConf = new SparkConf().setAppName("test spark sql Job");
		sparkConf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<OrderEntity> rdd = sc.textFile(path).map(
				  new Function<String, OrderEntity>() {
				    public OrderEntity call(String line) throws Exception {
				      String[] parts = line.split(" ");

				      OrderEntity orderEntity = new OrderEntity();
				      orderEntity.setOrderNo(Integer.parseInt(parts[0]));
				      orderEntity.setOrderTypeNo(Integer.parseInt(parts[1]));
				      orderEntity.setAmount(Double.parseDouble(parts[2]));
				      return orderEntity;
				    }
				  });
		DataFrame df = sqlContext.createDataFrame(rdd, OrderEntity.class);
		
		// Show the content of the DataFrame
		// df.show();
		
		// Print the schema in a tree format
		// df.printSchema();
		
		// Select only the "amount" column
		// df.select("amount").show();
		// df.select("*").show();
		
		// total numeber of df 
		// System.out.println(df.count());
		
		// Select all record  but amount the age by 1
		// df.select(df.col("orderNo"), df.col("amount").plus(1)).show();

		// Select people greater than 60
		// df.filter(df.col("amount").gt(60)).show();
		
		// Select people less than 60
		// df.filter(df.col("amount").lt(60)).show();
		
		// Select people equal to 60
		// df.filter(df.col("amount").equalTo(60)).show();

		// groupBy orderNo
		// df.groupBy("orderNo").count().show();
		
		df.registerTempTable("order_table");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame df2 = sqlContext.sql("SELECT orderNo,orderTypeNo FROM order_table a WHERE a.amount>=60");

		df2.show();

	}
	
	//HiveContext
	@Deprecated
	public static void testSparkSqlInHive(String path) {
			
		SparkConf sparkConf = new SparkConf().setAppName("test spark sql Job");
		sparkConf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		//HiveContext sqlContext = new HiveContext(sc);

	}
	
}
