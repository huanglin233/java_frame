package com.hl.bigdata.scala.generics

/**
 * @author huanglin
 * @date 2022/03/10 22:48:36
 */
class Pair1[T <: Comparable[T]](val first : T, val second : T) {
  def smaller = if(first.compareTo(second) < 0) first else second
}

object Pair1 extends App {
    val p = new Pair1("F", "B");
    println(p.smaller);
}
