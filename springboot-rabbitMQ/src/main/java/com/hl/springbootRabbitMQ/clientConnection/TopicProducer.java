package com.hl.springbootRabbitMQ.clientConnection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.hl.springbootRabbitMQ.utils.RabbitMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Topic模式 消息生成者
 */
public class TopicProducer {
    public final static String EXCHANGE_NAME = "topics_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        RabbitMQUtil.checkServer();

        // 创建链接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 设置rabbitMQ相关信息
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setVirtualHost("my_vhost");
        // 创建一个新链接
        Connection connection = connectionFactory.newConnection();
        // 创建一个通道
        Channel channel = connection.createChannel();
        // 声明一个交换机
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        String[] routing_keys = new String[] {"usa.news", "usa.weather", "europe.news", "europe.weather" };
        String[] messages     = new String[] {"美国新闻", "美国天气", "欧洲新闻", "欧洲"};
        for(int i = 0; i < routing_keys.length; i++) {
            String routingKey = routing_keys[i];
            String message    = messages[i];
            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("utf-8"));
            System.out.println("发送消息到路由: " + routingKey + " -----> 内容: " + message);
        }
    }
}