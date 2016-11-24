package com.superh.hz.bigdata.scala.learn.basicCompoundType

import com.superh.hz.bigdata.scala.learn.function.FunctionCharacteristic

/**
  * Created by Administrator on 2016/7/14.
  */
object TestString {

  def main(args: Array[String]) {
    val floatVar = 12.456
    val intVar = 2000
    val stringVar = "Hello, Scala!"
    val fs = printf("The value of the float variable is " +
      "%f, while the value of the integer " +
      "variable is %d, and the string " +
      "is %s", floatVar, intVar, stringVar)
    println(fs)
    println(stringVar.length())
  }
}
