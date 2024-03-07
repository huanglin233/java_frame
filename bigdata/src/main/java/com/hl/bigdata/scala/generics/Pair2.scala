package com.hl.bigdata.scala.generics

/**
 * @author huanglin
 * @date 2022/03/10 22:39:44
 */
class Pair2[T](val first : T, val second : T){

  def replaceFirst[R >: T](newFirst : R) = new Pair2[R](newFirst, second);

  override def toString: String =  {
    "(" + first + "," + second + ")"
  }
}

object Pair2 extends App {
  val p = new Pair2("F", "S");
  println(p);
  println(p.replaceFirst("NF"));
  println(p);
//  (F,S)
//  (NF,S)
//  (F,S)
}
