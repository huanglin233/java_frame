package com.hl.cloud.order8000;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.hystrix.EnableHystrix;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
@EnableHystrix
public class HystrixOrder8000 {
    public static void main(String[] args) {
        SpringApplication.run(HystrixOrder8000.class, args);
    }
}
