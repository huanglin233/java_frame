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

/**
 * topic模式 消息消费者
 */
public class TopicCustomer1 {
    private final static String EXCHANGE_NAME = "topics_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        RabbitMQUtil.checkServer();

        // 为当前消费者取名字
        String name = "consumer-usa";
        // 创建链接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 设置Rabbit配置信息
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setVirtualHost("my_vhost");
        // 创建一个新链接
        Connection connection = connectionFactory.newConnection();
        // 创建一个通道
        Channel channel = connection.createChannel();
        // 声明交换机
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        // 获取一个临时队列
        String queue = channel.queueDeclare().getQueue();
        // 绑定队列信息
        channel.queueBind(queue, EXCHANGE_NAME, "usa.*");
        System.out.println(name + "等待接受消息");
        // DefaultConsumer类实现了Consumer接口，通过传入一个频道，
        // 告诉服务器我们需要那个频道的消息，如果频道中有消息，就会执行回调函数handleDelivery
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "utf-8");
                System.out.println(name + " 接收到消息 '" + message + "'");
            }
        };
       //自动回复队列应答 -- RabbitMQ中的消息确认机制
       channel.basicConsume(queue, true, consumer);
    }
}