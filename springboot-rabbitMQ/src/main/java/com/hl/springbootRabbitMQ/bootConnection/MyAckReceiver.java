package com.hl.springbootRabbitMQ.bootConnection;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

/**
 * 消费者创建监听器
 */
@Component
public class MyAckReceiver implements ChannelAwareMessageListener{

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        // 消息的唯一ID
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        try {
            String msg = message.toString();
            System.out.println("消息: " + msg);
            System.out.println("消息来自: " + message.getMessageProperties().getConsumerQueue());

            // 手动确认
            channel.basicAck(deliveryTag, true);
        } catch (Exception e) {
            // 拒绝策略
            channel.basicReject(deliveryTag, true);
            e.printStackTrace();
        }
    }
}