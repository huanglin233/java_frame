package com.hl.bigdata.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;


/**
 * @author huanglin
 * @date 2025/03/31 22:18
 */
public class FlinkKafkaTest {

    static Configuration conf = new Configuration();
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

    // 设置 Kafka 生产者属性


    public static void main(String[] args) throws Exception {
        DataStreamSource<String> source = env.fromElements("data1", "data2", "data3");
        Properties props = new Properties();

        source.sinkTo(
                KafkaSink.<String>builder()
                        .setBootstrapServers("127.0.0.1:9092")
                        .setKafkaProducerConfig(props)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("test")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                        .build()
        );

        env.execute("Flink sink to Kafka");
    }
}
