package com.superh.hz.bigdata.scala.learn.collections

/**
  * Created by Administrator on 2016/8/30.
  */
object TestCollections_Option {

  def main(args: Array[String]) {
    //testOptionStatement()
    testGetOrElse()
  }

  /*
   * 声明option
   */
  def testOptionStatement() :Unit = {
    val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF")

    println("red is ", colors.get("red"))
    println("blue is ", colors.get("blue"))

    def show(x: Option[String]) = x match {
      case Some(s) => s
      case None => "?"
    }

    println("red is ", show(colors.get("red")))
    println("blue is ", show(colors.get("blue")))

  }

  /*
   * option是否为空
   */
  def testGetOrElse() : Unit={
    val a:Option[Int] = Some(5)
    val b:Option[Int] = None

    println("a.getOrElse(0): " + a.getOrElse(0) )
    println("b.getOrElse(10): " + b.getOrElse(10) )
    println("a.getOrElse(0): " + a.isEmpty )
    println("b.getOrElse(10): " + b.isEmpty )

  }

}
