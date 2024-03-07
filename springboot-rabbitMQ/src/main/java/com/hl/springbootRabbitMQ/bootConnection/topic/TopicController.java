package com.hl.springbootRabbitMQ.bootConnection.topic;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TopicController {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @GetMapping("/send/topic")
    public String sendByTopic() {
        String msg = "hello topic";
        rabbitTemplate.convertAndSend("topic_exchange", "topic.A", msg + "by topic.A");
        rabbitTemplate.convertAndSend("topic_exchange", "topic.AASS", msg + "by topic.AASS");

        return "send success";
    }
}