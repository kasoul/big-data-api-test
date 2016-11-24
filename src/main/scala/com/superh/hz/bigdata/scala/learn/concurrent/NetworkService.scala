package com.superh.hz.bigdata.scala.learn.concurrent

import java.net.ServerSocket
import java.util.concurrent.{ExecutorService, Executors}

/**
  * Created by Administrator on 2016/9/2.
  */
class NetworkService(port: Int, poolSize: Int) {
  val serverSocket = new ServerSocket(port)
  val pool: ExecutorService = Executors.newFixedThreadPool(poolSize)

  def startup() {
    println("start server")
    try {
      while (true) {
        // This will block until a connection comes in.
        val socket = serverSocket.accept()
        pool.execute(new Handler(socket))
      }
    } finally {
      pool.shutdown()
      println("shutdown server")
    }
  }
}
