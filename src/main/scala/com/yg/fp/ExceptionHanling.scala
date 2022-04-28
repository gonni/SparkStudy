package com.yg.fp

object ExceptionHanling {

  def failingFn(i: Int): Int = {
    val y: Int = throw new Exception("Fail!")
    try {
      val x = 42 + 5
      x + y
    } catch {
      case e: Exception => 43
    }
  }

  def main(args: Array[String]): Unit = {
    println("Active System on Scala")

    println("returnInt " + failingFn(10))
  }
}
