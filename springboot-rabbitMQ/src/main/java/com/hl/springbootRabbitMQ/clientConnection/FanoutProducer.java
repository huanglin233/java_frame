package com.hl.springbootRabbitMQ.clientConnection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.hl.springbootRabbitMQ.utils.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 *  Fanout模式 消息提供者
 */
public class FanoutProducer {
    private final static String EXCHANG_NAME = "fanout_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        RabbitMQUtil.checkServer();

        // 创建工厂链接
        ConnectionFactory factory = new ConnectionFactory();
        // 设置RabbitMQ地址
        factory.setHost("localhost");
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setVirtualHost("my_vhost");
        // 创建一个链接
        Connection newConnection = factory.newConnection();
        // 创建一个通道
        Channel createChannel = newConnection.createChannel();
        createChannel.exchangeDeclare(EXCHANG_NAME, "fanout");

        for(int i = 0; i < 100; i++) {
            String message = "direct 消息 " + i;
            // 发送消息到rabbitMQ队列中
            createChannel.basicPublish(EXCHANG_NAME, "", null, message.getBytes("UTF-8"));
            System.out.println("已发发送消息 : " + message);
        }

        // 关闭通道和链接
        createChannel.close();
        newConnection.close();
    }
}