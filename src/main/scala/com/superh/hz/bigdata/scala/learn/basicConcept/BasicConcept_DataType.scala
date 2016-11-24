package com.superh.hz.bigdata.scala.learn.basicConcept

//import scala.xml._
//import scala.collection.mutable.HashMap
//import scala.collection.immutable.{TreeSet, TreeMap}

/**
  * Created by Administrator on 2016/6/14.
  * scala syntax basic.data type
  */
object BasicConcept_DataType {
  def main(args: Array[String]) {
    //testChar()
    //testFloat()
   //testTypeAssignment()
    var bac : BasicConcept_AccessControl = new BasicConcept_AccessControl()
    bac.testFun()
  }

  /*
   * test data type => Int
   */
  def testInt() : Unit ={
    var t = 2
    println("Hello World!")
    println(t)
    t = 3
    println(t)
    //var symbol is variable
    //val symbol is not variable
    val bealoon1 = t == 2
    println(bealoon1)
  }

  def testFloat() : Unit ={
    var t = 2.0
    println(t)
    t = 2.0f
    t = 1e30d
    println(t)
    t = .1
    val t2 = 1
    val t3 = t + t2
    println(t3)
  }

  def testNull() {
    val t = null
    println(t==null)
    println(t!=null)
  }

  def testString(): Unit ={
    val s = """the present string ,
           spans three
           lines."""
    print(s)
  }

  def testTypeAssignment(): Unit ={
    val tInt1 = 5

    val tInt2 = 0x7FFFFFFA
    println(tInt2)
    val tInt3 = 0xFFFFFFF0
    println(tInt3)
    val tInt4 = 0x8000000A
    println(tInt4 + tInt1)
    //int type is in [-2^16,2^16-1]
    val tLong1 = 0xFFFFFFFAl
    println(tLong1)
    val tLong2 = 0777l
    println(tLong2)
  }

  def testChar(): Unit ={
    var t = 'a'
    println(t)
    //一个字符由2个字节表示,字母数据等常用字符和ASCII码一样，第一个byte为00，后一个byte为ASCII码的byte。中文在第一个byte改变
    println(t.toByte)
    t = 'A'
    println(t.toByte)
    t = '1'
    println(t.toByte)
    //t = 'U0041'
    val t2 = '\"'
    println(t2)
    println(t2.toByte)
  }
}
