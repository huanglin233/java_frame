package com.hl.springbootRabbitMQ.bootConnection.direct;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DirectController {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @GetMapping("/direct/send")
    public String sendByDirect() {
        String msg = "hello";
        rabbitTemplate.convertAndSend("direct_queue", "my.direct.routing",msg);

        return "send success";
    }
}