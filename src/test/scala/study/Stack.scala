package study

class Stack[+T] {
  def push[S >: T](elem: S): Stack[S] = new Stack[S] {
    override def top: S = elem
    override def pop: Stack[S] = Stack.this
    override def toString: String =
      elem.toString + " " + Stack.this.toString
  }
  def top: T = sys.error("no element on stack")
  def pop: Stack[T] = sys.error("no element on stack")
  override def toString: String = ""
}

trait Similar {
  def isSimilar(x: Any): Boolean
}
case class MyInt(x: Int) extends Similar {
  def isSimilar(m: Any): Boolean =
    m.isInstanceOf[MyInt] &&
      m.asInstanceOf[MyInt].x == x
}

case class ListNode[+T](h: T, t: ListNode[T]) {
  def head: T = h
  def tail: ListNode[T] = t
  def prepend[U >: T](elem: U): ListNode[U] =
    ListNode(elem, this)
}

object VariancesTest extends App {
  val empty: ListNode[Null] = ListNode(null, null)
  val strList: ListNode[String] = empty.prepend("hello")
    .prepend("world")
  val anyList: ListNode[Any] = strList.prepend(12345)
  println(empty)
  println(strList)
  println(anyList)


//  var s: Stack[Any] = new Stack().push("hello")
//  s = s.push(new Object())
//  s = s.push(7)
//  println(s)


//  def findSimilar[T <: Similar](e: T, xs: List[T]): Boolean =
//    if (xs.isEmpty) false
//    else if (e.isSimilar(xs.head)) true
//    else findSimilar[T](e, xs.tail)
//  val list: List[MyInt] = List(MyInt(1), MyInt(2), MyInt(3))
//  println(findSimilar[MyInt](MyInt(4), list))
//  println(findSimilar[MyInt](MyInt(2), list))
//
//  val list1: List[MyInt] = List(MyInt(1), MyInt(2), MyInt(3))
//
//  println("Full -> " + list1)
//  println("Tail -> " + list1.tail)
//  println("Tail2-> " + list1.tail)
}