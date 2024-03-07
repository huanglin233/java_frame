package com.hl.springbootKafka.client;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka消息消费者
 */
public class Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private static KafkaConsumer<String, String> consumer;

    /**
     * 初始化消费者
     */
    static {
        Properties properties = initConfig();
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(KafkaConfig.CONSUMER_TOPIC));
    }

    /**
     * 初始化配置
     */
    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaConfig.KAFKA_ADDRESS_COLLECTION);
        properties.put("group.id", KafkaConfig.CONSUMER_GROUP_ID);
        properties.put("enable.auto.commit", KafkaConfig.CONSUMER_ENABLE_AUTO_COMMIT); // 自动提交时间
        properties.put("auto.commit.interval.ms", KafkaConfig.CONSUMER_AUTO_COMMIT_INTERVAL_MS); // 自动提交间隔时间
        properties.put("session.timeout.ms", KafkaConfig.CONSUMER_SESSION_TIMEOUT_MS); // 连接超时时间
        properties.put("max.poll.records", KafkaConfig.CONSUMER_MAX_POLL_RECORDS); // 每次最大拉取数
        properties.put("auto.offset.reset", "earliest"); // 从最近的一次开始获取数据
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        return properties;
    }

    public static void main(String[] args) {
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(KafkaConfig.CONSUMER_POLL_TIME_OUT);
            records.forEach((ConsumerRecord<String, String> record)->{
                LOGGER.info("revice: key ==="+record.key()+" value ===="+record.value()+" topic ==="+record.topic());
            });
        }
    }
}