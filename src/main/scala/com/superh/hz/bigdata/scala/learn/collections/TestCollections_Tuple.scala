package com.superh.hz.bigdata.scala.learn.collections

/**
  * Created by Administrator on 2016/8/30.
  */
object TestCollections_Tuple {

  def main(args: Array[String]) {
    testTupleStatement()
  }

  /*
  * 声明tuple
  */
  def testTupleStatement() :Unit ={
    val t = (1, "hello", Console)
    val t1 = new Tuple1(1)
    val t2 = new Tuple2(1,"hello")
    val t3 = new Tuple3(1,"hello",Console)
    val tnum = (4,3,2,1)
    println( tnum._1 + tnum._2 + tnum._3 + tnum._4 )
    t3.productIterator.foreach{ i =>println("Value = " + i )}

    println("Concatenated String: " + t2.toString() )
    val t2new = t2.swap
    println("Concatenated String: " + t2new.toString() )

  }

}
