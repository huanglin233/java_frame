package com.hl.cloud.payment8004.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import cn.hutool.core.lang.UUID;

@RestController
public class PaymentController {

    @Value("${server.port}")
    private String serverPort;

    @GetMapping("/payment/consul")
    public String paymentZk() {
        System.out.println("do payment");
        return "spring_cloud_consul_payment : port = " + serverPort + " " + UUID.randomUUID().toString();
    }
}