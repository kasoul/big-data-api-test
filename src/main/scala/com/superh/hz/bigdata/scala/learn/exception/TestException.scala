package com.superh.hz.bigdata.scala.learn.exception

import java.io.{FileNotFoundException, FileReader, IOException}

/**
  * Created by Administrator on 2016/8/31.
  */
object TestException {
  def main(args: Array[String]) {
    testHandleException()
  }

  def testHandleException() :Unit = {
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException => {
        println("Missing file exception")
      }
      case ex: IOException => {
        println("IO Exception")
      }
    } finally {
      println("Exiting finally...")
    }

  }
}

