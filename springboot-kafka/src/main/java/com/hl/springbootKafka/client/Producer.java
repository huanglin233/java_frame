package com.hl.springbootKafka.client;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka消费者
 */
public class Producer {
    static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    private static KafkaProducer<String, String> producer = null;

    /**
     * 初始化生产者
     */
    static {
        Properties properties = initConfig();
        producer = new KafkaProducer<>(properties);
    }

    /**
     * 初始化配置
     */
    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaConfig.KAFKA_ADDRESS_COLLECTION);
        /*
         * 1. acks=0   意味着生产者能够通过网络吧消息发送出去，那么就认为消息已成功写入Kafka 一定会丢失一些数据 
         * 2. acks=1   意味着首领在疏导消息并把它写到分区数据问津是会返回确认或者错误响应，还是可能会丢数据 
         * 3. acks=all 意味着首领在返回确认或错误响应之前，会等待所有同步副本都收到消息。如果和min.insync.replicas参数结合起来，，
         *             就可以决定在返回确认前至少有多个副本能够收到消息。 但是效率较低。可以通过一部模式和更大的批次来加快速度，但这样做会降低吞吐量
         */
        properties.put("acks", "all");
        properties.put("retires", 0); // 重试次数
        properties.put("batch.size", 16384); // 批量大小
        properties.put("key.serializer", StringSerializer.class.getName()); // key序列化类型
        properties.put("value.serializer", StringSerializer.class.getName()); // value序列化类型

        return properties;
    }

    public static void main(String[] args) {
        // 消息实体
        ProducerRecord<String, String> record = null;
        for(int i = 0; i < 100; i++) {
            record = new ProducerRecord<String, String>(KafkaConfig.PRODUCER_TOPIC, "key", "the value hello " + i);
            // 发送消息
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null != exception) {
                        LOGGER.info("send error : " + exception.toString());
                    } else {
                        LOGGER.info("offest: " + metadata.offset() + ",partition: " + metadata.partition());
                    }
                }
            });
        }
        producer.close();
    }
}