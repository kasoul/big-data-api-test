package com.superh.hz.bigdata.scala.learn.regex

import scala.util.matching.Regex

/**
  * Created by Administrator on 2016/8/30.
  */
object TestRegex {

  def main(args: Array[String]) {
    testSimplePattern()
  }

  /*
   * 简单的正则表达式
   */
  def testSimplePattern() : Unit={
    val pattern = "Scalaa".r
    val pattern2 = new Regex("(S|s)cala")
    val str = "Scala is Scalable and cool"

    println(pattern findFirstIn str)
    println((pattern2 findAllIn str).mkString(","))
    println(pattern2 replaceFirstIn(str, "Java"))
    println(pattern2 replaceAllIn(str, "Java"))

  }
}
