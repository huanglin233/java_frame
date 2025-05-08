package com.hl.bigdata.flink.hdfs.scala

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * @author huanglin
 * @date 2025/05/08 21:01
 */
object KafkaToHdfs {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val source = KafkaSource.builder[String]()
      .setBootstrapServers("127.0.0.1:9092")
      .setTopics("hdfs001")
      .setGroupId("kafkaToHdfs")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks[String](), "kafkaToHdfs")
      .setParallelism(1)
      .addSink(StreamingFileSink.forRowFormat[String](new Path("hdfs://127.0.0.1:2181"), new SimpleStringEncoder[String]("UTF-8"))
        .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd"))
        .withRollingPolicy(DefaultRollingPolicy.builder()
        .withMaxPartSize(10 * 1024 * 1024)
        .withRolloverInterval(60 * 1000L)
        .withRolloverInterval(5 * 6 * 1000L)
        .build())
        .withOutputFileConfig(new OutputFileConfig("part-", ".txt"))
        .build()
      )

    env.execute("kafkaToHdfs")
  }
}
