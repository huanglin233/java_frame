package com.hl.cloud.payment8002.controller;

import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.hl.cloud.entities.Payment;
import com.hl.cloud.payment8002.service.impl.PaymentServiceImpl;
import com.hl.cloud.utils.CommonResult;

@RestController
public class PaymentController {
    private static Logger logger = LoggerFactory.getLogger(PaymentController.class);

    @Autowired
    private PaymentServiceImpl paymentServiceImpl;

    @Value("${server.port}")
    private String serverPort;

    @PostMapping("/payment/create")
    public CommonResult<Integer> create(@RequestBody Payment payment, HttpServletRequest request){
        int result = paymentServiceImpl.create(payment);
        logger.info("-----插入结果: " + result);

        if(result == 1) {
            return new CommonResult<Integer>(200, "插入数据成功-8002", result);
        } else {
            return new CommonResult<Integer>(444, "插入数据失败-8002", null);
        }
    }

    @GetMapping("/payment/get/{id}")
    public CommonResult<Payment> getPaymentById(@PathVariable("id") Long id) {
        Payment payment = paymentServiceImpl.getPaymentById(id);
        logger.info("-----查询结果 : " + payment);

        if(payment != null) {
            return new CommonResult<Payment>(200, "查询成功-8002", payment);
        } else {
            return new CommonResult<Payment>(444, "没有查找对应数据,查找失败-8002", null);
        }
    }

    @GetMapping("/payment/lb")
    public String getPaymentLB() {
        return serverPort;
    }

    @GetMapping("/payment/openfegin/timeout")
    public String paymentFeginTimeOut() {
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        return serverPort;
    }
}