package com.hl.bigdata.flink

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * @author huanglin
 * @date 2024/03/08 15:03
 */
object WordCount {

  def main(args : Array[String]) {
    val env: ExecutionEnvironment = ExecutionEnvironment.createLocalEnvironment(1);
    val text   = env.readTextFile("D:\\project\\java_frame\\bigdata\\src\\main\\resources\\people.txt")
    val counts = text.flatMap(_.toLowerCase().split(",") filter(_.nonEmpty)).map{(_, 1)}
      .groupBy(0)
      .sum(1);

    counts.print();
    counts.writeAsText("D:\\project\\java_frame\\bigdata\\src\\main\\resources\\peopleOut.text", WriteMode.OVERWRITE);
    env.execute();
  }
}
