package com.superh.hz.bigdata.scala.learn.basicCompoundType

/**
  * Created by Administrator on 2016/7/14.
  */
class BigOffRoadVehicle(override val carT: String, override val carC: String,val carP: Double)
  extends OffRoadVehicle (carT, carC) {
  override val carType: String = carT + "[]"
  override val carColor: String = carC + "[]"
  var carPrice = carP

  override def run(): Unit = {
    println("a " + carColor + "-"+ carType + "-$" + carPrice + " run..")
  }


}
