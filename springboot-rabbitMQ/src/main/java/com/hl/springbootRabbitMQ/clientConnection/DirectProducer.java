package com.hl.springbootRabbitMQ.clientConnection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.hl.springbootRabbitMQ.utils.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Direct模式 消息提供者 
 */
public class DirectProducer {
    public final static String QUEUE_NAME = "direct_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        RabbitMQUtil.checkServer();

        // 创建链接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 设置RabbitMQ相关信息
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setVirtualHost("my_vhost");
        // 创建一个新连接
        Connection connection = connectionFactory.newConnection();
        // 创建一个通道
        Channel channel = connection.createChannel();
//        channel.exchangeDeclare(QUEUE_NAME, "direct");

        for(int i = 0; i < 100; i++) {
            String messages = "direct消息: " + i;
            channel.basicPublish(QUEUE_NAME, "", null, messages.getBytes("utf-8"));
            System.out.println("发送消息: " + messages);
        }

        // 关闭链接通道
        channel.close();
        connection.close();
    }
}