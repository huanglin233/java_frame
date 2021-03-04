package com.hl.springbootRabbitMQ.bootConnection.direct;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DirectRabbitMQConfig {

    /**
     * 交换机
     */
    @Bean
    public DirectExchange myDirectExchange() {
        // 参数意义:
        // name: 名称
        // durable: true 重启的时候不需要创建新的交换机
        // autoDelete: 自动删除
        return new DirectExchange("direct_queue", true, false);
    }

    /**
     * 队列
     */
    @Bean
    public Queue myDirectQueue() {
        return new Queue("direct_queue", true);
    }

    /**
     * 绑定
     */
    @Bean
    public Binding bindingDirect() {
        return BindingBuilder.bind(myDirectQueue()).to(myDirectExchange()).with("my.direct.routing");
    }
}