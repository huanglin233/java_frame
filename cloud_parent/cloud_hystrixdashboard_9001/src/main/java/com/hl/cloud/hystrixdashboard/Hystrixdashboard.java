package com.hl.cloud.hystrixdashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.hystrix.dashboard.EnableHystrixDashboard;

@SpringBootApplication
@EnableHystrixDashboard
public class Hystrixdashboard {
    public static void main(String[] args) {
        SpringApplication.run(Hystrixdashboard.class, args);
    }
}
