package com.hl.bigdata.scala.`trait`

import java.util.Date

/**
 * @author huanglin
 * @date 2022/03/10 21:05:00
 */
trait DogLogger extends AnimalLogger {

  println("DogLogger构造器");

  val trait_name0 = " Dog ";

  override def log(msg : String): Unit = {
    super.log(" " + new Date() + trait_name0 + msg);
  }

  abstract override def abstract_log(msg: String): Unit = {
    super.log(" " + new Date() + trait_name0 + msg);

  }
}
