package com.superh.hz.bigdata.scala.learn.basicConcept

/**
  * Created by Administrator on 2016/6/14.
  * scala syntax basic.operator
  */
object BasicConcept_Operator {


  def main(args: Array[String]) {

    //testArithmeticOperator()

    //testRelationalOperator()

    //testLogicalOperator()

    testAssignmentOperator()

  }

  def testArithmeticOperator(): Unit ={
    val a = 10
    val b = 20
    val c = 25
    //val d = 25
    println("a + b = " + (a + b) )
    println("a - b = " + (a - b) )
    println("a * b = " + (a * b) )
    println("b / a = " + (b / a) )
    println("b % a = " + (b % a) )
    println("c % a = " + (c % a) )
  }

  def testRelationalOperator(): Unit ={
    val a = 10
    val b = 20
    println("a == b = " + (a == b) )
    println("a != b = " + (a != b) )
    println("a > b = " + (a > b) )
    println("a < b = " + (a < b) )
    println("b >= a = " + (b >= a) )
    println("b <= a = " + (b <= a) )
  }

  def testLogicalOperator(): Unit ={
    val a = true
    val b = false

    println("a && b = " + (a&&b) )

    println("a || b = " + (a||b) )

    println("!(a && b) = " + !(a && b) )
  }

  def testAssignmentOperator(): Unit ={
    var a = 10
    val b = 20
    var c = 0
    
    c = a + b
    println("c = a + b  = " + c )

    c += a 
    println("c += a  = " + c )

    c -= a 
    println("c -= a = " + c )

    c *= a 
    println("c *= a = " + c )

    a = 10
    c = 15
    c /= a 
    println("c /= a  = " + c )

    a = 10
    c = 15
    c %= a 
    println("c %= a  = " + c )

    c <<= 2 
    println("c <<= 2  = " + c )

    c >>= 2 
    println("c >>= 2  = " + c )

    c >>= 2 
    println("c >>= a  = " + c )

    c &= a 
    println("c &= 2  = " + c )

    c ^= a 
    println("c ^= a  = " + c )

    c |= a 
    println("c |= a  = " + c )
  }

}
