package com.hl.bigdata.flink.kafka

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * @author huanglin
 * @date 2025/03/31 23:00
 */
object FlinkKafkaTest_Scala{

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val source = env.fromElements("data1_scala", "data2_scala");
    source.sinkTo(KafkaSink.builder[String]()
      .setBootstrapServers("127.0.0.1:9092")
      .setKafkaProducerConfig(new java.util.Properties())
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("test")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
      .build()
    )

    env.execute("flink sink to kafka")
  }
}
