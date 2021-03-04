package com.hl.cloud.order8000.controller;

import javax.annotation.Resource;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.hl.cloud.entities.Payment;

@RestController
public class OrderController {
    private static final String PATMENT_URL = "http://cloud-provider-zk-payment";

    @Resource
    private RestTemplate restTemplate;

    @GetMapping("/consumer/payment/zk")
    public String create(Payment payment) {
        return restTemplate.getForObject(PATMENT_URL + "/payment/zk", String.class);
    }
}