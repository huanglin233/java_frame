package com.hl.bigdata.flink.stream.scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @author huanglin
 * @date 2025/02/15 16:15
 */
object Run {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = List(10, 11, 12)
    val stream = env.fromCollection(data).map(_ + 1)
    stream.print()
    env.execute()
  }
}
