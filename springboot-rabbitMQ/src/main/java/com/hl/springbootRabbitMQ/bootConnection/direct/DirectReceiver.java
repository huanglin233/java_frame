package com.hl.springbootRabbitMQ.bootConnection.direct;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class DirectReceiver {

    @RabbitHandler
    @RabbitListener(queues = "direct_queue")
    public void process(String msg) {
        System.out.println("boot receiver : " + msg);
    }
}