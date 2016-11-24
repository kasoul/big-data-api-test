package com.superh.hz.bigdata.scala.learn.collections

/**
  * Created by Administrator on 2016/8/30.
  */
object TestCollections_Set {

  def main(args: Array[String]) {
    //testSetStatement()
    testSetConnect()
  }

  /*
  * 声明set
  */
  def testSetStatement() :Unit = {
    val fruit = Set("apples", "oranges", "pears")
    val nums: Set[Int] = Set()

    println( "Head of fruit : " + fruit.head )
    println( "Tail of fruit : " + fruit.tail )
    println( "Check if fruit is empty : " + fruit.isEmpty )
    println( "Check if nums is empty : " + nums.isEmpty )

    val fruit1 = fruit.+("banana")
    println( "fruit1 is : " + fruit1 )
    val fruit2 = fruit.-("apples")
    println( "fruit1 is : " + fruit2 )
  }

  /*
 * set的交集和并集
 */
  def testSetConnect() :Unit = {
    val fruit1 = Set("apples", "oranges", "pears")
    val fruit2 = Set("mangoes", "banana","apples")

    // use two or more sets with ++ as operator
    var fruit = fruit1 ++ fruit2
    println( "fruit1 ++ fruit2 : " + fruit )

    // use two sets with ++ as method
    fruit = fruit1.++(fruit2)
    println( "fruit1.++(fruit2) : " + fruit )

    val num1 = Set(5,6,9,20,30,45)
    val num2 = Set(50,60,9,20,35,55)
    val num3 = num1.++(num2)
    println( " num3 is  : " + num3 )
    println( " num3.max is  : " + num3.max )
    println( " num3.min is  : " + num3.min )
    println( " num3.sum is  : " + num3.sum )

    // find common elements between two sets
    println( "num1.&(num2) : " + num1.&(num2) )
    println( "num1.intersect(num2) : " + num1.intersect(num2) )

    // find elements in left set but not in right set
    println( "num1.&~(num2) : " + num1.&~(num2) )

  }


}
