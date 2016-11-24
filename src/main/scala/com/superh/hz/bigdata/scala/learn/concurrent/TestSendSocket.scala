package com.superh.hz.bigdata.scala.learn.concurrent

import java.io.IOException
import java.net.Socket

/**
  * Created by Administrator on 2016/9/2.
  */
object TestSendSocket {

  def main(args: Array[String]) {
    testSendSocket()
  }

  def testSendSocket() :Unit={

    try {
      val socket = new Socket("localhost", 2020)
      val istream = socket.getInputStream()
      val ostream = socket.getOutputStream()
      val clientData = "test client data.[end]"
      ostream.write(clientData.getBytes())
      ostream.flush()
      val b = new Array[Byte](256)
      val sb = new StringBuffer()
      while (istream.read(b) != -1) {
        sb.append(new String(b))
      }
      println(sb.toString())
      ostream.close()
      istream.close()
      socket.close();
    }catch {
      case ex: IOException => {
        ex.printStackTrace()
      }
    }finally {

    }

  }

}
