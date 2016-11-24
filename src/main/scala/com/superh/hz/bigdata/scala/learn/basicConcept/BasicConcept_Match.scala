package com.superh.hz.bigdata.scala.learn.basicConcept

/**
  * Created by Administrator on 2016/7/14.
  */
object BasicConcept_Match {

  def main(args: Array[String]) {
    println(matchValue())
    println(matchValue(2))
    println(matchType("test string"))

  }

  //match 返回值
  def matchValue(): String ={
    val times = 1

    val value = times match {
      case 1 => "one"
      case 2 => "two"
      case _ => "some other number"
    }

    value
  }

  //match 返回值
  def matchValue(x:Int): String ={
    x match {
      case i if i == 1 => "one"
      case i if i == 2 => "two"
      case _ => "some other number"
    }
  }

  //匹配类型
  def matchType(o: Any): Any = {
    o match {
      case i: Int if i < 0 => i - 1
      case i: Int => i + 1
      case d: Double if d < 0.0 => d - 0.1
      case d: Double => d + 0.1
      case text: String => text + "||s"
    }
  }

}
