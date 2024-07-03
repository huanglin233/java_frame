package com.hl.bigdata.flink.stream.scala.source

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * @author huanglin
 * @date 2024/07/03 17:20
 */
class MySourceWorld extends RichParallelSourceFunction[String]{

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while(true) {
      sourceContext.collect("aa bb cc dd ee")
      println("产生一条数据")
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = ???
}
