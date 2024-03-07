package com.hl.bigdata.scala.`trait`

/**
 * @author huanglin
 * @date 2022/03/09 21:44:50
 */
trait Logger {

  println("Logger构造器");

  def log(msg: String);

  def abstract_log(msg : String);

  def log2(msg: String): Unit = {
    println(msg);
  }
}
