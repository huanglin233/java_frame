package com.hl.bigdata.flink.mysql.scala

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * @author huanglin
 * @date 2025/04/02 21:30
 */
object KafkaToMysql_Scala {

  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val source: KafkaSource[PageView_Scala] = KafkaSource.builder[PageView_Scala]()
      .setBootstrapServers("127.0.0.1:9092")
      .setTopics("to_mysql")
      .setGroupId("to_mysql")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new PageViewDeserializationSchema_Scala())
      .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks[PageView_Scala](), "kafka-source")
      .keyBy(_.pageUrl)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
      .apply(new WindowFunction[PageView_Scala, List[PageView_Scala], String, TimeWindow ]() {

        override def apply(key: String, window: TimeWindow, input: Iterable[PageView_Scala], out: Collector[List[PageView_Scala]]): Unit = {
          var list: List[PageView_Scala] = List()
          for (e <- input) {
            list = e :: list
          }
          out.collect(list)
        }
      }).addSink(new MysqlSink_Scala())

    env.execute("flink kafka to mysql")
  }
}
