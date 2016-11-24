package com.superh.hz.bigdata.scala.learn.basicConcept

import Array._

/**
  * Created by Administrator on 2016/7/4.
  */
object BasicConcept_Array {

  def main(args: Array[String]) {
    //println(1 != 1)
    testArrayStatement()
    //testRange()
    //testMultiDimensionalArray()
  }

  private def testArrayStatement(): Unit ={
    var aString1:Array[String] = new Array[String](3)
    aString1(0) = "Huang"
    aString1(1) = "Li"
    aString1(2) = "Wang"


    var aString2:Array[String] = Array("Zara", "Nuha", "Ayan")

    var aString3 =  concat( aString1, aString2)

    // Print all the array elements
    for ( x <- aString3 ) {
      println( x )
    }
  }

  def testRange(): Unit ={
    var myList1 = range(10, 20, 2)
    var myList2 = range(10,20)

    // Print all the array elements
    for ( x <- myList1 ) {
      print( " " + x )
    }
    println()
    for ( x <- myList2 ) {
      print( " " + x )
    }
  }

  def testMultiDimensionalArray() :Unit={
    var myMatrix = ofDim[Int](3,3)

    // build a matrix
    for (i <- 0 to 2) {
      for ( j <- 0 to 2) {
        myMatrix(i)(j) = j
      }
    }

    // Print two dimensional array
    for (i <- 0 to 2) {
      for ( j <- 0 to 2) {
        print(" " + myMatrix(i)(j))
      }
      println()
    }
  }
}
