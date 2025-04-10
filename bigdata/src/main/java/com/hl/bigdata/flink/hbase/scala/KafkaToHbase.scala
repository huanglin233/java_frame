package com.hl.bigdata.flink.hbase.scala

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author huanglin
 * @date 2025/04/10 21:16
 */
object KafkaToHbase {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val source : KafkaSource[String] = KafkaSource.builder[String]()
      .setBootstrapServers("127.0.0.1:9092")
      .setTopics("to_hbase")
      .setGroupId("to_hbase")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks[String](), "kafka source")
      .addSink(new HbaseSink())

    env.execute("kafka to sink")
  }
}
