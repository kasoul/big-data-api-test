package com.superh.hz.bigdata.scala.learn.collections

import spire.math.UInt

/**
  * Created by Administrator on 2016/7/26.
  */
object TestCollections_List {

  def main(args: Array[String]) {
    //testListStatement()
    //testListConnect()
    //testFill()
    //testTabulate()
    testReverse()
  }

  /*
   * 声明List
   */
  def testListStatement() :Unit ={

    // List of Strings
    val fruit: List[String] = List("apples", "oranges", "pears")

    // List of Integers
    val nums: List[Int] = List(1, 2, 3, 4)
    val num: List[Int] = List(3)

    // Empty List.
    val empty: List[Nothing] = List()

    // Two dimensional list
    val dim: List[List[Int]] =
      List(
        List(1, 0, 0),
        List(0, 1, 0),
        List(0, 0, 1)
      )

    // List of Strings
    val fruit2 = "apples" :: ("oranges" :: ("pears" :: Nil))

    // List of Integers
    val nums2 = 1 :: (2 :: (3 :: (4 :: Nil)))

    // Empty List.
    val empty2 = Nil

    // Two dimensional list
    val dim2 = (1 :: (0 :: (0 :: Nil))) ::
      (0 :: (1 :: (0 :: Nil))) ::
      (0 :: (0 :: (1 :: Nil))) :: Nil

    println( "Head of fruit : " + fruit2.head )
    println( "Tail of fruit : " + fruit2.tail )
    println( "Check if fruit is empty : " + fruit2.isEmpty )
    println( "Check if empty2 is empty : " + empty2.isEmpty )

  }

  /*
   * List的连接
   */
  def testListConnect() :Unit ={
    val fruits1 = "apple" :: ("banana" :: Nil)
    val fruits2 = List("orange","peach")
    val fruits3 = fruits1 ::: fruits2
    val fruits4 = List.concat(fruits2,fruits1)

    println("fruits3 is " + fruits3)
    println("fruits4 is " + fruits4)

  }

  /*
   * List的填充
   */
  def testFill() :Unit ={
    val fruits1 = List.fill(3)("apples")
    val num1 = List.fill(5)(5)

    println("fruits1 is " + fruits1)
    println("num1 is " + num1)

  }

  /*
   * List的制表
   */
  def testTabulate() :Unit ={
    val squares = List.tabulate(6)(n => n * n)
    println( "squares : " + squares  )

    //
    val mul = List.tabulate( 4,5 )( _ * _ )
    println( "mul : " + mul  )
  }

  /*
   * List的反转
   */
  def testReverse() :Unit ={
    val fruit = "apples" :: ("oranges" :: ("pears" :: Nil))
    println( "Before reverse fruit : " + fruit )
    println( "After reverse fruit : " + fruit.reverse )
  }
}
