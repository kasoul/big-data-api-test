package com.superh.hz.bigdata.scala.learn.basicConcept

import scala.util.control._

/**
  * Created by Administrator on 2016/6/14.
  * scala syntax basic.circle :while and for syntax
  */
object BasicConcept_Circle {
  def main(args: Array[String]) {

    //testWhileSyntax()

    //testDoWhileSyntax()

    //testForSyntax()

    //testForSyntax2()

    //testForWithFilter()

    //testForWithYield()

    testBreakSyntax()



  }

  /*
   * test data type => Int
   */
  def testWhileSyntax() : Unit ={

    // Local variable declaration:
    var a = 10

    // while loop execution
    while( a < 20 ) {
      println("Value of a: " + a)
      a = a + 1
    }

  }

  def testDoWhileSyntax() : Unit ={

    // Local variable declaration:
    var a = 10

    // do loop execution
    do{
      println( "Value of a: " + a )
      a = a + 1
    }while( a < 20 )

  }

  def testForSyntax() {

    //var a = 0 //这个变量与循环控制同名，在循环里是看不到的
    // for loop execution with a range
    // 循环变量a默认新建变量，无需关键字val或var
    // 箭头< - 操作者被称为生成器，这样命名是因为它是从一个范围产生单个数值
    for( a <- 1 to 10){
      println( "Value of a: " + a )

    }
    println( "i am split line --------------------------")
    for( a <- 11 until 15){
      println( "Value of a: " + a )

    }

  }

  def testForSyntax2(): Unit ={
    // for loop execution with multiple range
    for( a <- 1 to 3; b <- 1 to 3){
      println( "Value of a: " + a )
      println( "Value of b: " + b )
    }
  }



  def testForWithFilter(): Unit ={

    val numList = List(1,2,3,4,5,6,7,8,9,10)

    // for loop execution with multiple filters
    for( a <- numList
         if a != 3; if a < 8 ){
      println( "Value of a: " + a )
    }
  }

  def testForWithYield(): Unit ={

    val numList = List(1,2,3,4,5,6,7,8,9,10)

    // for loop execution with a yield
    val retVal = for{ a <- numList
                      if a != 3; if a < 8
    }yield a

    // Now print returned values using another loop.
    for( a <- retVal){
      println( "Value of a: " + a )
    }
  }

  //内循环break，外循环继续
  def testBreakSyntax(): Unit ={

    val numList1 = List(1,2,3,4,5)
    val numList2 = List(11,12,13)

    val outer = new Breaks
    val inner = new Breaks

    outer.breakable {
      for( a <- numList1){
        println( "Value of a: " + a )
        inner.breakable {
          for( b <- numList2){
            println( "Value of b: " + b )
            if( b == 12 ){
             // inner.break
              outer.break
            }
          }
        } // inner breakable
      }
    } // outer breakable.
  }
}
