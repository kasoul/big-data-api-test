package com.superh.hz.bigdata.scala.learn.basicCompoundType

/**
  * Created by Administrator on 2016/7/14.
  */
object TestCollection {

  def main(args: Array[String]) {
    testList()
  }

  def testList(): Unit ={
    // List of Strings
    val fruit: List[String] = List("apples", "oranges", "pears")

    // List of Integers
    val nums: List[Int] = List(1, 2, 3, 4)

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
  }

}
