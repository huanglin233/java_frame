package com.hl.springbootRabbitMQ.clientConnection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.hl.springbootRabbitMQ.utils.RabbitMQUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * direct模式 消息消费者
 */
public class DirectCustomer {
    public final static String QUEUE_NAME = "direct_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        RabbitMQUtil.checkServer();

        // 创建链接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 配置参数信息
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setVirtualHost("my_vhost");
        // 创建一个新连接
        Connection connection = connectionFactory.newConnection();
        // 创建一个通道
        Channel channel = connection.createChannel();
        // 表示声明一个交换机
        channel.exchangeDeclare(QUEUE_NAME, "direct", true, false, false, null);
        // 表示声明一个队列
        channel.queueDeclare(QUEUE_NAME, false, false, true, null);
        // 建立绑定关系
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "");
        System.out.println("等待接受消息");
        // DefaultConsumer类实现了Consumer接口，通过传入一个频道，
        // 告诉服务器我们需要那个频道的消息，如果频道中有消息，就会执行回调函数handleDelivery
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                    byte[] body) throws IOException {
                String message = new String(body, "utf-8");
                System.out.println("接受到消息: " + message);
            }
        };
        // 自动回复队列应答 -- RabbitMQ中的消息确认机制
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}