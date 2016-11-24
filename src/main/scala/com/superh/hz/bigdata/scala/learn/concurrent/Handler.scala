package com.superh.hz.bigdata.scala.learn.concurrent

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

/**
  * Created by Administrator on 2016/9/2.
  */
class Handler(socket: Socket) extends Runnable {
    def message = (Thread.currentThread.getName() + "\n").getBytes

    def run() {
      val istream = socket.getInputStream()
      val ostream = socket.getOutputStream()
      val b = new Array[Byte](256)

      val sb = new StringBuffer()
      if (istream.read(b) != -1) {
        sb.append(new String(b))
        ostream.write(message)
        ostream.flush()
      }
      println(sb.toString())

      ostream.close()
      istream.close()
    }
}
