package com.hl.springbootRabbitMQ.bootConnection.topic;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TopicRabiitMQConfig {

    /**
     * 交换机
     */
    @Bean
    public TopicExchange myTopicExchange() {
        // 参数意义:
        // name: 名称
        // durable: true 重启的时候不需要创建新的交换机
        // autoDelete: 自动删除
        return new TopicExchange("topic_exchange", true, false);
    }

    /**
     * 队列
     */
    @Bean
    public Queue myTopicQueueA() {
        return new Queue("topicQueueA", true);
    }

    @Bean
    public Queue myTopicQueueB() {
        return new Queue("topicQueueB", true);
    }

    /**
     * 绑定路由键为topic.A
     */
    @Bean
    public Binding bindingQueueA() {
        return BindingBuilder.bind(myTopicQueueA()).to(myTopicExchange()).with("topic.A");
    }

    @Bean
    public Binding bindingQueueB() {
        return BindingBuilder.bind(myTopicQueueB()).to(myTopicExchange()).with("topic.#");
    }
}