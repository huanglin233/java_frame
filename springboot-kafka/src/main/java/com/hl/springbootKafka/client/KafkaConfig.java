package com.hl.springbootKafka.client;

import java.time.Duration;

/**
 * kafka客户端链接相关配置信息
 */
public class KafkaConfig {
    public static final String   KAFKA_ADDRESS_COLLECTION         = "192.168.56.101:9092";    // kafka地址
    public static final String   CONSUMER_TOPIC                   = "test";                  // 消费者链接的topic
    public static final String   PRODUCER_TOPIC                   = "test";                  // 生产者链接的topic
    public static final String   CONSUMER_GROUP_ID                = "1";                     // groupID
    public static final String   CONSUMER_ENABLE_AUTO_COMMIT      = "true";                  // 是否自动提交(消费者)
    public static final String   CONSUMER_AUTO_COMMIT_INTERVAL_MS = "30000";                 // 自动提交间隔,单位毫毫秒秒
    public static final String   CONSUMER_SESSION_TIMEOUT_MS      = "30000";                 // 链接超时时间
    public static final int      CONSUMER_MAX_POLL_RECORDS        = 10;                      // 每次最大拉取数
    public static final Duration CONSUMER_POLL_TIME_OUT           = Duration.ofMillis(3000); // 拉取超时时间
}