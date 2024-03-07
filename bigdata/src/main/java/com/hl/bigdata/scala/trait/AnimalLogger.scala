package com.hl.bigdata.scala.`trait`

/**
 * @author huanglin
 * @date 2022/03/10 20:28:58
 */
trait AnimalLogger extends Logger {

  println("AnimalLogger构造器");

  // 特质中未被初始化的字段在具体的子类中必须被重写。
  val name : String;

  def log(msg : String): Unit = {
    println(msg)
  }
}
