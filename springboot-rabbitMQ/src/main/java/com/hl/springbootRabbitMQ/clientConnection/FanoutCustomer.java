package com.hl.springbootRabbitMQ.clientConnection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.hl.springbootRabbitMQ.utils.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

import cn.hutool.core.util.RandomUtil;

/**
 * Fanout模式 消息消费者
 */
public class FanoutCustomer {
    private final static String EXCHANG_NAME = "fanout_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        RabbitMQUtil.checkServer();
        String consumerName = "consumer" + RandomUtil.randomString(5);

        // 创建链接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setVirtualHost("my_vhost");
        // 创建一个链接
        Connection newConnection = connectionFactory.newConnection();
        // 创建一个通道
        Channel createChannel = newConnection.createChannel();
        //  声明交换机
        createChannel.exchangeDeclare(EXCHANG_NAME, "fanout");
        // 获取一个临时队列
        String queue = createChannel.queueDeclare().getQueue();
        // 队列与交换机绑定
        createChannel.queueBind(queue, EXCHANG_NAME, "");
        System.out.println(consumerName + " : 等待接受消息");

        // DefaultConsumer类实现了Consumer接口，通过传入一个频道，
        // 告诉服务器我们需要那个频道的消息，如果频道中有消息，就会执行回调函数handleDelivery
        Consumer consumer = new DefaultConsumer(createChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "utf-8");
                System.out.println(consumerName + " 接受到消息 " + message);
            }
        };

        // 自动回复队列应答 -- RabbitMQ中的消息确认机制
        createChannel.basicConsume(queue, true, consumer);
    }
}