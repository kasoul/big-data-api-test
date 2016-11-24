package com.superh.hz.bigdata.scala.learn.collections

/**
  * Created by Administrator on 2016/8/30.
  */
object TestCollections_Map {

  def main(args: Array[String]) {
    //testMapStatement()
    testErgodicMap()
  }

  /*
   * 声明map
   */
  def testMapStatement() :Unit = {
    val colors = Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")

    val nums: Map[Int, Int] = Map()

    println( "Keys in colors : " + colors.keys )
    println( "Values in colors : " + colors.values )
    println( "Check if colors is empty : " + colors.isEmpty )
    println( "Check if nums is empty : " + nums.isEmpty )

    val colors2 = Map("blue" -> "#0033FF",
      "yellow" -> "#FFFF00",
      "red" -> "#FF0000")

    // use two or more Maps with ++ as operator
    var colors3 = colors ++ colors2
    println( "colors1 ++ colors2 : " + colors )

    // use two maps with ++ as method
    colors3 = colors.++(colors2)
    println( "colors1.++(colors2)) : " + colors )

  }

  /*
   * 遍历map
   */
  def testErgodicMap() : Unit = {
    val colors = Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")
    colors.keys.foreach{ i =>
      print( "Key = " + i )
      println(" Value = " + colors(i) )}

    if( colors.contains( "red" )){
      println("Red key exists with value :"  + colors("red"))
    }else{
      println("Red key does not exist")
    }
    if( colors.contains( "maroon" )){
      println("Maroon key exists with value :"  + colors("maroon"))
    }else{
      println("Maroon key does not exist")
    }
  }

}
