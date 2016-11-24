package com.superh.hz.bigdata.scala.learn.function

/**
  * Created by Administrator on 2016/7/1.
  */
object FunctionConcept {

  def main(args: Array[String]) {
    val fc: FunctionClass = new FunctionClass()
    fc.testObjectFunction(5, 5)
    fc.testFunction(5, 6)
    println(addInt(5, 7))
  }

  def addInt(a: Int, b: Int): Int = {
    var sum: Int = 0
    sum = a + b

    return sum
  }

}
