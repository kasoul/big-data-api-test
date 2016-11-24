package com.superh.hz.bigdata.scala.learn.concurrent

import java.net.Socket


/**
  * Created by Administrator on 2016/9/2.
  */
object TestThread {

  def main(args: Array[String]) {

    new NetworkService(2020,2).startup()

  }

  def testNewThread() : Unit={
    new Thread(new Runnable {
      def run() {
        println("hello world")
      }
    }).start()
  }


}
