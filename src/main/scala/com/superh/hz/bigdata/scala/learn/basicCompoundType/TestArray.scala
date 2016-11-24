package com.superh.hz.bigdata.scala.learn.basicCompoundType

//引入Array的所有函数
import Array._

/**
  * Created by Administrator on 2016/7/14.
  */
object TestArray {

  def main(args: Array[String]) {

    //test()
    //testOfDIM()
    testRange()

  }

  def test(): Unit ={
    val z1 = new Array[String](3)
    z1(0) = "aa"
    z1(1) = "bb"
    z1(2) = "cc"
    val z2 = Array("a","b","c")
    println(z1(2))
    println(z2.length)

    // Print all the array elements
    for ( x <- z1 ) {
      print( x + "," )
    }

    println("-----------------------I am split line------------------------")
    // Print all the array elements
    for ( x <- 0 to z1.length-1 ) {
      print( z1(x) + "," )
    }

  }

  def testOfDIM():  Unit  ={
    val myMatrix = ofDim[Int](3,3)

    // build a matrix
    for (i <- 0 to 2) {
      for ( j <- 0 to 2) {
        myMatrix(i)(j) = j;
      }
    }

    // Print two dimensional array
    for (i <- 0 to 2) {
      for ( j <- 0 to 2) {
        print(" " + myMatrix(i)(j));
      }
      println();
    }
  }

  def testRange():Unit={
    val myList1 = range(10, 20, 2)
    val myList2 = range(10,20)

    // Print all the array elements
    for ( x <- myList1 ) {
      print( " " + x )
    }
    println()
    for ( x <- myList2 ) {
      print( " " + x )
    }
  }



}
