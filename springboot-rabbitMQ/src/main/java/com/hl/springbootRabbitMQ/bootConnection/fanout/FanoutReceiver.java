package com.hl.springbootRabbitMQ.bootConnection.fanout;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class FanoutReceiver {

    @RabbitHandler
    @RabbitListener(queues = "fanoutQueueA")
    public void receiverA(String msg) {
        System.out.println("boot receicerA : " + msg);
    }

    @RabbitHandler
    @RabbitListener(queues = "fanoutQueueB")
    public void receiverB(String msg) {
        System.out.println("boot receicerB : " + msg);
    }

    @RabbitHandler
    @RabbitListener(queues = "fanoutQueueC")
    public void receiverV(String msg) {
        System.out.println("boot receicerC : " + msg);
    }
}