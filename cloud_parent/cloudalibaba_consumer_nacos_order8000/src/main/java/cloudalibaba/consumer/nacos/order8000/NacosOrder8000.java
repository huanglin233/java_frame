package cloudalibaba.consumer.nacos.order8000;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
//@EnableDiscoveryClient
@EnableFeignClients
public class NacosOrder8000 {
    public static void main(String[] args) {
        SpringApplication.run(NacosOrder8000.class, args);
    }
}