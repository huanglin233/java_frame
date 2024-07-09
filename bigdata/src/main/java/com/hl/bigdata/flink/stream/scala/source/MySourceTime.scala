package com.hl.bigdata.flink.stream.scala.source

import org.apache.commons.lang3.RandomUtils
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
 * @author huanglin
 * @date 2024/07/05 11:04
 */
class MySourceTime extends RichSourceFunction[String]{

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while(true) {
      val key = "hello" + RandomUtils.nextInt(1, 5);
      sourceContext.collect(key + "," + (System.currentTimeMillis() - RandomUtils.nextLong(1000, 30000)));
      Thread.sleep(1000);
    }
  }

  override def cancel(): Unit = ???
}
