package com.superh.hz.bigdata.scala.learn.basicCompoundType

/**
  * Created by Administrator on 2016/7/14.
  */
class OffRoadVehicle(val carT: String, val carC: String) extends Car with Mendable {

  override val carType: String = carT
  val carColor: String = carC

  override def run(): Unit = {
    println("a " + carColor + "-"+ carType + " run..")
  }

  override def mend(): Unit = {
    println("a car is mending..")
  }


}
