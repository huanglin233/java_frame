package com.hl.springbootKafka.stream;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * kakfa stream 测试demo, 进行单词统计
 */
@SpringBootTest
public class WordCount {

    public static void main(String[] args) {
        // 配置信息
        Properties props = new Properties();
        // Streams应用Id
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordCount");
        // Kafka集群地址
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.1:9092");
        // 指定序列化和反序列化类型
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 创建一个topology构建器，在kakfa中计算逻辑被定义为连接的处理器节点的拓扑。
        StreamsBuilder builder = new StreamsBuilder();
        // 使用topology构建器创建一个源流，指定源topic
        KStream<String, String> stream = builder.stream("test");

        stream.map((key, value) -> {
            List<String> asList = Arrays.asList(value.split("\\W+"));
            final AtomicInteger i = new AtomicInteger(0);
            asList.stream().forEach(v -> {
                if(v.equals("the")) {
                    i.incrementAndGet();
                }
            });
            return new KeyValue<>(key, String.valueOf(i));
        }).to("test2");

        // 创建Streams客户端
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}