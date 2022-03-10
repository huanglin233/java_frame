package com.hl.bigdata.scala.`trait`

import java.util.Date

/**
 * @author huanglin
 * @date 2022/03/10 21:05:07
 */
trait BigDogLogger extends AnimalLogger {

  println("BigDogLogger构造器");

  val trait_name1 = " Big Dog "

  override def log(msg : String): Unit = {
    super.log(" " + new Date() + trait_name1 + msg);
  }

  abstract override def abstract_log(msg: String): Unit = {
    super.log(" " + new Date() + trait_name1 + msg);
  }
}
