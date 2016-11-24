package com.superh.hz.bigdata.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * Created by Administrator on 2016/7/4.
  */
object NetworkWordCount {

  def main(args: Array[String]) {

    //buildSocketStreaming()

    //buildSocketStreamWithUpdateState()

    //buildSocketStreamWithUpdateState2()

    //buildRecoverableSocketStreamWithUpdateState()

    //buildSocketStreamingWithTransform()

    buildSocketStreamingWithWindows()


  }

  def buildSocketStreaming(): Unit ={

    val sc = new SparkConf().setMaster("local[*]").setAppName("streaming test world count")
    val ssc = new StreamingContext(sc,Seconds(5))
    val inputDStream = ssc.socketTextStream("192.168.182.130",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    //val words = inputDStream.flatMap(s => s.split(" "))
    val wordCounts = inputDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

  def buildSocketStreamWithUpdateState(): Unit ={
    val sc = new SparkConf().setMaster("local[*]").setAppName("streaming test world count update statte")
    val ssc = new StreamingContext(sc,Seconds(5))
    val inputDStream = ssc.socketTextStream("192.168.182.130",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    val words = inputDStream.flatMap(_.split(" ")).map((_,1))

    def updateFuction(newValues: Seq[Int],runningCount:Option[Int]): Option[Int] = {

        val newValue = runningCount.getOrElse(0) + newValues.size
        return Some[Int](newValue)

    }



    val wordCountsWithState = words.updateStateByKey(updateFuction)

    wordCountsWithState.print()

    ssc.checkpoint("hdfs://node2:8020/user/hadoop/hc/checkpoint")
    ssc.start()
    ssc.awaitTermination()


  }

  def buildSocketStreamWithUpdateState2(): Unit ={
    val sc = new SparkConf().setMaster("local[*]").setAppName("streaming test world count update statte")

    val ssc = new StreamingContext(sc,Seconds(5))
    val initialRDD = ssc.sparkContext.parallelize(List(("",1)))
    val inputDStream = ssc.socketTextStream("192.168.182.130",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    val words = inputDStream.flatMap(_.split(" ")).map((_,1))

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val wordCountsWithState = words.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

    wordCountsWithState.print()

    ssc.checkpoint("hdfs://node2:8020/user/hadoop/hc/checkpoint")
    ssc.start()
    ssc.awaitTermination()


  }

  def buildRecoverableSocketStreamWithUpdateState(): Unit ={

    val checkpointDirectory = "hdfs://node2:8020/user/hadoop/hc/checkpoint"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(checkpointDirectory))

    ssc.start()
    ssc.awaitTermination()

  }

  def createContext(checkpointDirectory :String): StreamingContext={
    val sc = new SparkConf().setMaster("local[*]").setAppName("streaming test world count update statte")
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint(checkpointDirectory)

    val inputDStream = ssc.socketTextStream("192.168.182.130",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    val wordCounts = inputDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    def updateFuction(newValues: Seq[Int],runningCount:Option[Int]): Option[Int] = {

      val newValue = runningCount.getOrElse(0) + newValues.size
      return Some[Int](newValue)

    }

    val wordCountsWithState = wordCounts.updateStateByKey(updateFuction)

    wordCountsWithState.print()

    ssc
  }

  def buildSocketStreamingWithTransform(): Unit ={

    val sc = new SparkConf().setMaster("local[*]").setAppName("streaming test world count")
    val ssc = new StreamingContext(sc,Seconds(5))
    val spamInfoRDD = ssc.sparkContext.parallelize(List(("a",1),("b",1),("c",1)))
    val inputDStream = ssc.socketTextStream("192.168.182.130",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    //val words = inputDStream.flatMap(s => s.split(" "))
    val wordCounts = inputDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    val filterWord = wordCounts.transform(rdd => {
      rdd.join(spamInfoRDD).filter(e => !(e._2._2==1&&e._2._1==1))
    })

    filterWord.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

  def buildSocketStreamingWithWindows(): Unit ={

    val sc = new SparkConf().setMaster("local[*]").setAppName("streaming test world count")
    val ssc = new StreamingContext(sc,Seconds(2))

    val inputDStream = ssc.socketTextStream("192.168.182.130",9999,StorageLevel.MEMORY_AND_DISK_SER_2)

    val wordCounts = inputDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    val windowsWords = wordCounts.countByWindow(Seconds(6),Seconds(2))

    windowsWords.print()

    ssc.checkpoint("hdfs://node2:8020/user/hadoop/hc/checkpoint")
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

}
