package com.hl.bigdata.scala.generics

/**
 * @author huanglin
 * @date 2022/03/10 23:52:02
 */
//在Scala中，如果你想标记某一个泛型可以隐式的转换为另一个泛型，可以使用：[T <% Comparable[T]] ，由于Scala的Int类型没有实现Comparable接口，所以我们需要将Int类型隐式的转换为RichInt类型
class Pair3[T <% Comparable[T]](val first : T, val second : T) {

 def smaller = if(first.compareTo(second) < 0) first else second;

  override def toString: String =  {
    "(" + first + "," + second + ")"
  }
}

object Pair3 extends App {
  val p = new Pair3(1, 2);
  println(p.smaller);
}