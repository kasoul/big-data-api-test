package com.superh.hz.bigdata.spark.scala.core


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}


/**
  * Created by Administrator on 2016/7/4.
  */
object SimpleSparkApp {

  def main(args: Array[String]) {

    val logFile = "C:\\Users\\Administrator\\Desktop\\testdata\\test-spark.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2)

    val broadcastVar = sc.broadcast(5)
    val accmu = sc.accumulator(0)

    //testFilterRDD(logData)

    //testMapRDD(logData)

    //testDefinedFunction(logData)

    //testBroadcast(logData, broadcastVar)

    testAccumulator(logData,accmu)

  }

  def testFilterRDD(logData:RDD[String]): Unit ={

    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

  }

  def testMapRDD(logData:RDD[String]): Unit ={

    val lineLengths = logData.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    println("total length : %s".format(totalLength))

  }

  def testDefinedFunction(logData:RDD[String]): Unit ={

    val function1 = (s: String) => s.length
    val lineLengths = logData.map(function1)
    def function2(a:Int,b:Int): Int ={
      return a+b
    }
    val totalLength = lineLengths.reduce(function2)
    println("total length : %s".format(totalLength))

  }

  def testBroadcast(logData:RDD[String],broadcastVar:Broadcast[Int]): Unit ={

    val function1 = ((s: String) => s.length + broadcastVar.value)
    val lineLengths = logData.map(function1)
    def function2(a:Int,b:Int): Int ={
      return a+b
    }
    val totalLength = lineLengths.reduce(function2)
    println("total length : %s".format(totalLength))

  }

  def testAccumulator(logData:RDD[String],accumulator:Accumulator[Int]): Unit ={

    val function1 = ((s: String) => accumulator.add(s.length))
    logData.foreach(function1)
    println("total length : %s".format(accumulator.value))

  }



}
