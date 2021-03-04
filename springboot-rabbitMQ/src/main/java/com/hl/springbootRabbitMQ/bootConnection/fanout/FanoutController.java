package com.hl.springbootRabbitMQ.bootConnection.fanout;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FanoutController {

    @Autowired
    RabbitTemplate rabbitTemplate;

    @GetMapping("/fanout/send")
    public String sendByFanout() {
        String msg = "hello fanout";
        rabbitTemplate.convertAndSend("fanout_exchange", null, msg);

        return "send success";
    }
}