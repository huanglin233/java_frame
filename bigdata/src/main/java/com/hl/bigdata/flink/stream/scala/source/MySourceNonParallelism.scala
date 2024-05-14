package com.hl.bigdata.flink.stream.scala.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author huanglin
 * @date 2024/03/12 15:32
 */
class MySourceNonParallelism extends SourceFunction[Int]{

  var sum: Int           = 0
  var isRunning: Boolean = true


  override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
    while(isRunning) {
      sourceContext.collect(sum)
      sum = sum + 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = isRunning = false
}
