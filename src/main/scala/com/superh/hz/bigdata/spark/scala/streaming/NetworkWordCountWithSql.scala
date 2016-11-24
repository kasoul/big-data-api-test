package com.superh.hz.bigdata.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Accumulator
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Created by Administrator on 2016/7/4.
  */
object NetworkWordCountWithSql {

  def main(args: Array[String]) {

    buildSocketStreamingWithSql()

  }

  def buildSocketStreamingWithSql(): Unit ={

    val sc = new SparkConf().setMaster("local[*]").setAppName("streaming test world count")
    val ssc = new StreamingContext(sc,Seconds(5))
    val inputDStream = ssc.socketTextStream("192.168.182.130",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    //val words = inputDStream.flatMap(s => s.split(" "))
    val wordCounts = inputDStream.flatMap(_.split(" "))

    wordCounts.foreachRDD { (rdd: RDD[String], time: Time) =>
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.registerTempTable("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        sqlContext.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()

    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}

/** Case class for converting RDD to DataFrame */
case class Record(word: String)
