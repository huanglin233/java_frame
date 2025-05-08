package com.hl.bigdata.flink.hdfs.java;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;;

/**
 * 读取kafka数据写入hdfs中
 * @author huanglin
 * @date 2025/05/07 21:38
 */
public class KafkaToHdfs {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setTopics("hdfs001")
                .setGroupId("kafkaToHdfs")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .setParallelism(1)
                .addSink(StreamingFileSink.forRowFormat(new Path("hdfs://127.0.0.1:2181/flink_kafka_out"), new SimpleStringEncoder<String>("UTF-8"))
                        // 按小时创建目录
                        .withBucketAssigner(new DateTimeBucketAssigner<String>("yyyy-MM-dd"))
                        // 设置 Rolling Policy：当文件大小超过 10MB 或每 1 分钟没有写入或文件已打开 5 分钟时滚动
                        .withRollingPolicy(DefaultRollingPolicy.builder()
                                .withMaxPartSize(10 * 1024 * 1024) // 10MB
                                .withRolloverInterval(60 * 1000)
                                .withInactivityInterval(5 * 60 * 1000)
                                .build())
                        .withOutputFileConfig(new OutputFileConfig("part-", ".txt"))
                        .build()
                );

        env.execute("KafkaToHdfs");
    }
}
