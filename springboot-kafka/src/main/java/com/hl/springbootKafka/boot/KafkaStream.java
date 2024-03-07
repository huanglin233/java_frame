package com.hl.springbootKafka.boot;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStream {
//
//    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
//    public KafkaStreamsConfiguration kStreamsConfigs() {
//        Map<String, Object> props = new HashMap<>();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test233");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.1:9092");
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
//        return new KafkaStreamsConfiguration(props);
//    }
    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder){
        KStream<String, String> stream = streamsBuilder.stream("test4");
        System.out.println("xxxx");
        stream.map((key,value) -> {
            value+=" --test--";
            return new KeyValue<>(key,value);
        }).to("test3"); 
        return stream;
    }

    @Bean
    public KStream<String, String> kStream2(StreamsBuilder streamsBuilder){
        KStream<String, String> stream = streamsBuilder.stream("test");
        final AtomicInteger i = new AtomicInteger(0);
        stream.map((key, value) -> {
            List<String> asList = Arrays.asList(value.split("\\W+"));
            asList.stream().forEach(v -> {
                if(v.equals("the")) {
                    i.incrementAndGet();
                }
                System.out.println("x:"+key+" y:"+v);
            });
            return new KeyValue<>(key, String.valueOf(i));
        }).groupByKey().windowedBy(TimeWindows.of(Duration.ofSeconds(5)).advanceBy(Duration.ofSeconds(1))).count().toStream().foreach((k, v) -> {
//            System.out.println("x:"+k+" y:"+v);
//            System.out.println("=============>" + k + v);
            System.out.println("i = " + i);
        }); // 创建一个跳跃时间窗，窗口大小5s，步长1s

        return stream;
    }
}