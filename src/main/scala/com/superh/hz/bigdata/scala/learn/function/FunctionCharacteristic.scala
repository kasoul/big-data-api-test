package com.superh.hz.bigdata.scala.learn.function

import java.util.Date

/**
  * Created by Administrator on 2016/7/4.
  */
object FunctionCharacteristic {


  def main(args: Array[String]) {

    //printAandB(3,5)
    //printAandB(b=10,a=6)

    //println("9! is  :" + factorial(9))

    //println(addInt())
    //println(addInt(2,3))

    //println(apply(f,5))

    //println(factorial2(4))

    //lamadaExpression()

    //function_()

    //closure()

    //addOne(100)

    testComposeFunction()

  }

  //参数顺序可以根据函数定义时参数名来确定，没有参数名则采用从左到右顺序方式
  def printAandB(a:Int,b:Int): Unit ={
    println("a is :" + a)
    println("b is :" + b)
  }

  //递归阶乘
  def factorial(n: BigInt): BigInt = {
    if (n <= 1)
      1
    else
      n * factorial(n - 1)
  }

  //可以设定默认的参数值,如果没有设置默认的参数值则必须传值
  def addInt( a:Int=5, b:Int=7 ) : Int = {
    var sum:Int = 0
    sum = a + b
    sum
  }

  //定义高阶函数，函数作为参数传入另一个函数中
  def apply(f:Int => String,va:Int): String = f(va)

  def f(v:Int): String = {
    return "double v is: " + v*2
  }

  //测试嵌套函数，阶乘*accumulator
  def factorial2(i: Int): Int = {
    def fact(i: Int, accumulator: Int): Int = {
      if (i <= 1)
        accumulator
      else
        fact(i - 1, i * accumulator)
    }
    fact(i, 2)
  }
  //定义匿名函数，即lamada表达式，用关键词val定义
  def lamadaExpression(): Unit ={
    val mul = (x: Int, y: Int) => x+y
    println(mul(3,4))

    val mul2 = (s: String) => s.contains("huangchao")
    println(mul2("huangchao in hangzhou"))

    val userDir = () => { System.getProperty("user.dir") }
    println(userDir())
  }

  //定义延迟传参合_通配符,柯里化函数
  def function_() {
    val date = new Date
    def log(date: Date, message: String)  = {
      println(date + "----" + message)
    }
    val logWithDateBound = log(date, _ : String)

    logWithDateBound("message1" )
    Thread.sleep(1000)
    logWithDateBound("message2" )
    Thread.sleep(1000)
    logWithDateBound("message3" )
  }

  //测试闭包函数,函数的闭包，函数调用外部的变量。
  def closure() {
    println( "muliplier(1) value = " +  multiplier(1) )
    println( "muliplier(2) value = " +  multiplier(2) )
  }
  var factor = 3
  val multiplier = (i:Int) => i * factor

  def addOne(m: Int): Int = m + 1



  def testComposeFunction() :Unit ={
    def f(s: String) = "f(" + s + ")"
    def g(s: String) = "g(" + s + ")"
    val fComposeG = f _ compose g _
    println(fComposeG("yay"))
    val fAndThenG = f _ andThen g _
    println(fAndThenG("aya"))
  }

}
