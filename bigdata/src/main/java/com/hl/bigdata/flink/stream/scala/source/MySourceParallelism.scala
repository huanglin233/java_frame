package com.hl.bigdata.flink.stream.scala.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * @author huanglin
 * @date 2024/03/12 17:34
 */
class MySourceParallelism extends RichParallelSourceFunction[Int]{

  var count: Int = 0
  var isRunning: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
    while(isRunning) {
      sourceContext.collect(count)
      count = count + 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = isRunning = false

  override def open(parameters: Configuration): Unit = {
    println("open multiple source")
    super.open(parameters)
  }
}
