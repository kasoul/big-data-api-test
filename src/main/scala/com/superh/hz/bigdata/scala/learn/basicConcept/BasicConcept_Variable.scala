package com.superh.hz.bigdata.scala.learn.basicConcept

/**
  * Created by Administrator on 2016/6/14.
  * scala syntax basic.variable
  */
object BasicConcept_Variable {

  val ss = "成员变量"

  def main(args: Array[String]) {
    //testVal()
    //testVar()
    println(ss)
    testFunParameter(ss);
  }

  def testVar(): Unit ={
    val s : String = "foo"
    val s2: Int = 1
    var s3 : String = new String()
    s3 = "world"
    //var s  = "foo"
    //var s2  = 1
    println(s)
    println(s2)
    println(s3)

  }

  def testVal(): Unit ={
    val s : String = "foo"
    val s2 : Int = 1
    println(s)
    println(s2)

    val (myVar1: Int, myVar2: String) = Pair(40, "Foo")
    println(myVar1 + "," + myVar2)
    val (myVar3, myVar4) = Pair(40, "Foo")
    println(myVar3 + "," + myVar4)
  }

  def testFunParameter(string: String ): Unit ={
    println("方法的参数：" + string)
  }


}
