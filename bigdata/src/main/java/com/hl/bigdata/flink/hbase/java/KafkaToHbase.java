package com.hl.bigdata.flink.hbase.java;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author huanglin
 * @date 2025/04/07 22:09
 */
public class KafkaToHbase {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setTopics("to_hbase")
                .setGroupId("to_hbase1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source").setParallelism(1)
                .addSink(new HbaseSink()).setParallelism(2);

        env.execute("kafka to hbase");
    }
}
