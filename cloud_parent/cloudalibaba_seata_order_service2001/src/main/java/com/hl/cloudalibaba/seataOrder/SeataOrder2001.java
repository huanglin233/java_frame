package com.hl.cloudalibaba.seataOrder;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class) // 取消数据源自动创建
@EnableDiscoveryClient
@EnableFeignClients
@MapperScan(value = "com.hl.cloudalibaba.seataOrder.dao")
public class SeataOrder2001 {
    public static void main(String[] args) {
        SpringApplication.run(SeataOrder2001.class, args);
    }
}