package com.hl.springbootRabbitMQ.bootConnection.fanout;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FanoutRabbitMQConfig {

    /**
     * 交换机
     */
    @Bean
    public FanoutExchange myFanoutExchange() {
        // 参数意义:
        // name: 名称
        // durable: true 重启的时候不需要创建新的交换机
        // autoDelete: 自动删除
        return new FanoutExchange("fanout_exchange", true, false);
    }

    /**
     * 队列
     */
    @Bean
    public Queue myFanoutQueueA() {
        return new Queue("fanoutQueueA", true);
    }

    @Bean
    public Queue myFanoutQueueB() {
        return new Queue("fanoutQueueB", true);
    }

    @Bean
    public Queue myFanoutQueueC() {
        return new Queue("fanoutQueueC", true);
    }

    /**
     * 绑定
     */
    @Bean
    public Binding bindingFanoutA() {
        return BindingBuilder.bind(myFanoutQueueA()).to(myFanoutExchange());
    }

    @Bean
    public Binding bindingFanoutB() {
        return BindingBuilder.bind(myFanoutQueueB()).to(myFanoutExchange());
    }

    @Bean
    public Binding bindingFanoutC() {
        return BindingBuilder.bind(myFanoutQueueC()).to(myFanoutExchange());
    }
}