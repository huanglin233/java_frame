package com.hl.springbootRabbitMQ.bootConnection.topic;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class TopicReceiver {

    @RabbitHandler
    @RabbitListener(queues = "topicQueueA")
    public void receiverQueueA(String msg) {
        System.out.println("topicQueueA receiver : " + msg);
    }

    @RabbitHandler
    @RabbitListener(queues = "topicQueueB")
    public void receiverQueueB(String msg) {
        System.out.println("topicQueueB receiver : " + msg);
    }
}