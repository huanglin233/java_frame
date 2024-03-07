package com.hl.bigdata.scala.`trait`

import java.io.PrintStream

/**
 * @author huanglin
 * @date 2022/03/10 21:09:42
 */
trait FileLogger extends Logger {

  val fileName : String;
  lazy val out = new PrintStream(fileName);

  override def log(msg: String): Unit = {
    out.println(msg);
    out.flush();
  }
}
