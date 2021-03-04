package com.hl.cloud.payment8001.controller;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.hl.cloud.entities.Payment;
import com.hl.cloud.payment8001.service.impl.PaymentServiceImpl;
import com.hl.cloud.utils.CommonResult;

@RestController
public class PaymentController {
    private static Logger logger = LoggerFactory.getLogger(PaymentController.class);

    @Autowired
    private PaymentServiceImpl paymentServiceImpl;

    @Autowired
    private DiscoveryClient discoveryClient;

    @Value("${server.port}")
    private String serverPort;

    @PostMapping("/payment/create")
    public CommonResult<Integer> create(@RequestBody Payment payment, HttpServletRequest request){
        int result = paymentServiceImpl.create(payment);
        logger.info("-----插入结果: " + result);

        if(result == 1) {
            return new CommonResult<Integer>(200, "插入数据成功-8001", result);
        } else {
            return new CommonResult<Integer>(444, "插入数据失败-8001", null);
        }
    }

    @GetMapping("/payment/get/{id}")
    public CommonResult<Payment> getPaymentById(@PathVariable("id") Long id, HttpServletRequest request) {
        String header = request.getHeader("X-Request-Id");

        Payment payment = paymentServiceImpl.getPaymentById(id);
        logger.info("-----查询结果 : " + payment);

        if(payment != null) {
            return new CommonResult<Payment>(200, "查询成功-8001, X-Request-Id = " + header, payment);
        } else {
            return new CommonResult<Payment>(444, "没有查找对应数据,查找失败-8001, X-Request-Id = " + header, null);
        }
    }

    @GetMapping("/payment/discoveryClient")
    public Object discoveryClient() {
        List<String> services = discoveryClient.getServices(); //拿到所有注册信息
        for(String server : services) {
            logger.info("-----server: " + server);
        }

        List<ServiceInstance> instances = discoveryClient.getInstances("CLOUD-PAYMENT-SERVICE");
        for(ServiceInstance instance : instances) {
            logger.info(instance.getServiceId()+"\t"+instance.getHost()+"\t"+instance.getPort()+"\t"+instance.getUri());  
        }

        return this.discoveryClient;
    }

    @GetMapping("/payment/lb")
    public String getPaymentLB() {
        return serverPort;
    }

    @GetMapping("/payment/openfegin/timeout")
    public String paymentFeginTimeOut() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        return serverPort;
    }
}