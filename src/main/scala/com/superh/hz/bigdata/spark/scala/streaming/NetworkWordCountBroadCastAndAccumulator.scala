package com.superh.hz.bigdata.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Accumulator
import org.apache.spark.streaming.{Seconds,StreamingContext,Time}

/**
  * Created by Administrator on 2016/7/4.
  */
object NetworkWordCountBroadCastAndAccumulator {

  def main(args: Array[String]) {

    buildSocketStreamingWithDropWord()

  }

  def buildSocketStreamingWithDropWord(): Unit ={

    val sc = new SparkConf().setMaster("local[*]").setAppName("streaming test world count")
    val ssc = new StreamingContext(sc,Seconds(5))
    val inputDStream = ssc.socketTextStream("192.168.182.130",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    //val words = inputDStream.flatMap(s => s.split(" "))
    val wordCounts = inputDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
      // Get or register the blacklist Broadcast
      // 不是某个RDD的广播，是全局的广播
      val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
      // Get or register the droppedWordsCounter Accumulator
      // 不是某个RDD的累加器，是全局的累加器
      val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
      // Use blacklist to drop words and use droppedWordsCounter to count them
      val counts = rdd.filter { case (word, count) =>
        if (blacklist.value.contains(word)) {
          droppedWordsCounter.add(count)
          false
        } else {
          true
        }
      }.collect().mkString("[", ", ", "]")
      val output = "Counts at time " + time + " " + counts
      println(output)
      println("Dropped " + droppedWordsCounter.value + " word(s) totally")

    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}

object WordBlacklist {

  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

object DroppedWordsCounter {

  @volatile private var instance: Accumulator [Long] = null

  def getInstance(sc: SparkContext): Accumulator [Long] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.accumulator(0L, "WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}

