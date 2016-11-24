package com.superh.hz.bigdata.scala.learn.basicCompoundType

/**
  * Created by Administrator on 2016/7/14.
  */
object TestClassAndTrait {

  def main(args: Array[String]) {
    new Car(){
      override val carType: String = "car"

      override def run(): Unit = {
        println("a car run..")
      }
    }.run()

    //继承和构造方法
    new OffRoadVehicle("suv","red").run()

    //构造方法重写
    val bigsuv = new BigOffRoadVehicle("big suv","blue",25.6d)
    //val bigsuv = new BigOffRoadVehicle("blue")
    bigsuv.run()
    bigsuv.mend()

    //构造方法重载
    val sportCar1 = new SportCar("sport car","white")
    sportCar1.run()
    val sportCar2 = new SportCar("sport car")
    sportCar2.run()
    val sportCar3 = new SportCar()
    sportCar3.run()

  }


}
