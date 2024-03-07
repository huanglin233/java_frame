package com.hl.cloud.order8000;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class OpenfeginOrder8000 {
    public static void main(String[] args) {
        SpringApplication.run(OpenfeginOrder8000.class, args);
    }
}