package com.hl.cloud.order8000.controller;


import javax.annotation.Resource;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.hl.cloud.entities.Payment;
import com.hl.cloud.order8000.service.PaymentService;
import com.hl.cloud.utils.CommonResult;

@RestController
public class PaymentController {

    @Resource
    private PaymentService paymentService;

    @GetMapping("/consumer/payment/get/{id}")
    public CommonResult<Payment> getPaymentById(@PathVariable("id") Long id) {
        CommonResult<Payment> paymentById = paymentService.getPaymentById(id);
        return paymentById;
    }

    @GetMapping("/consumer/payment/openfegin/timeout")
    public String getPaymentOpenfeginTimeOut() {
        return paymentService.paymentFeginTimeOut();
    }
}