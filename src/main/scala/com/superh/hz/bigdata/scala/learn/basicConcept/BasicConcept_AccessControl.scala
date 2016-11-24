package com.superh.hz.bigdata.scala.learn.basicConcept

/**
  * Created by Administrator on 2016/7/1.
  */
class BasicConcept_AccessControl {

  class Inner {
    private def f() { println("f") }
    class InnerMost {
      f() // OK
    }
  }

  class Inner2 {
    def f() { println("f2") }
    class InnerMost {
      f() // OK
    }
  }

  //class 里的main方法是何用？
  def main(args: Array[String]) {
    BasicConcept_DataType.testChar()
  }

  def testFun(): Unit = {
    //(new Inner).f() // Error: f is not accessible
    (new Inner2).f() //  f is not accessible
    BasicConcept_DataType.testChar()
    //BasicConcept_AccessControl bac = new BasicConcept_AccessControl()
  }


  class Super {
    protected def f() { println("f") }
    def f2() { println("f2") }
  }
  class Sub extends Super {
    f()
  }
  class Other {
    (new Super).f2() // Error: f is not accessible
  }

}
