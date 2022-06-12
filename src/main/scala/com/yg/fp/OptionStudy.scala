package com.yg.fp

object OptionStudy {
  def lookupByName(name: String) : Option[Employee] = {
    if(name.equals("Moma")) {
      Some(Employee("Moma", "HRD"))
    } else {
      None
    }
  }

  def lift[A, B](f: A => B): Option[A] => Option[B] = _ map f

  def maki[A](a: =>A): A = ???

  def main(args: Array[String]): Unit = {
    println("ReActive..")

    val man =  lookupByName("Moma")
    println("man =>" + man)
    val non = lookupByName("Mona")

    println("man ->" + man.map(_.department).getOrElse("Init"))
    println("non ->" + non.map(_.department).getOrElse("NA"))

    val abs0: Option[Double] => Option[Double] = lift(math.abs)

    println("abs ->" + abs0(Option(-10)))
  }
}

case class Employee(name: String, department: String)

