package com.hl.cloud.order8000;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class ConsulOrder8000 {
    public static void main(String[] args) {
        SpringApplication.run(ConsulOrder8000.class, args);
    }
}
