package com.hl.bigdata.flink.stream.scala.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * @author huanglin
 * @date 2024/11/20 21:44
 */
class MyWindowsSource extends SourceFunction[Int]{

  override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
    while(true) {
      sourceContext.collect(Random.nextInt(10))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {}
}
