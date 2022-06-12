package com.yg.fp

object StrictStudy {
  def if2[A](cond: Boolean, onTrue: () => A, onFalse: ()=>A): A =
    if(cond) onTrue() else onFalse()

  def if2n[A](cond: Boolean, onTrue: => A, onFalse: => A): A =
    if(cond) onTrue else onFalse


  def main(args: Array[String]): Unit = {
    println("Active")
  }
}


