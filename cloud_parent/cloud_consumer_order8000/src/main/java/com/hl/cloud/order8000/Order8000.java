package com.hl.cloud.order8000;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

@SpringBootApplication
@EnableEurekaClient
// **表示,访问CLOUD_PAYMENT_SERVICE的服务时,使用我们自定义的负载均衡算法**
//@RibbonClient(name = "CLOUD-PAYMENT-SERVICE", configuration = MySelfRule.class)
public class Order8000 {
    public static void main(String[] args) {
        SpringApplication.run(Order8000.class, args);
    }
}