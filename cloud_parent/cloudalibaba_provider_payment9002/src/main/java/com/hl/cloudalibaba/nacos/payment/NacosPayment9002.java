package com.hl.cloudalibaba.nacos.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class NacosPayment9002 {
    public static void main(String[] args) {
        SpringApplication.run(NacosPayment9002.class, args);
    }
}