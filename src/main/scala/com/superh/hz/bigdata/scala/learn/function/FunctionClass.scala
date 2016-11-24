package com.superh.hz.bigdata.scala.learn.function

/**
  * Created by Administrator on 2016/7/1.
  */
class FunctionClass {

  def testFunction(a:Int, b:Int): Unit ={
      println("value is " + ( a+b ) )
  }

  def testObjectFunction(a:Int, b:Int): Unit ={
    println("value is " + FunctionConcept.addInt(a,b) )
  }

}
