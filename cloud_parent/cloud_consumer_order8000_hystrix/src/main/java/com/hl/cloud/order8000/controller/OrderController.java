package com.hl.cloud.order8000.controller;

import javax.annotation.Resource;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.hl.cloud.order8000.service.PaymentHystirxService;

@RestController
//@DefaultProperties(defaultFallback = "paymentHystrixTimeOutGlobalHandler")
public class OrderController {

    @Resource
    private PaymentHystirxService paymentHystirxService;

    @GetMapping("/consumer/payment/hystrix/ok/{id}")
    public String paymentHystrixOk(@PathVariable("id") Integer id) {
        return paymentHystirxService.paymentInfo_ok(id);
    }

    @GetMapping("/consumer/payment/hystrix/timeout/{id}")
//    @HystrixCommand(fallbackMethod = "paymentHystrixTimeOutHandler", commandProperties = {
//            @HystrixProperty(name = "execution.isolation.thread.timeoutInMilliseconds", value = "3000")
//    })
//    @HystrixCommand #当使用全局的降级处理是需要加上次注解
    public String paymentHystrixTimeOut(@PathVariable("id") Integer id) {
        return paymentHystirxService.paymentInfoTimeOut(id);
    }

    public String paymentHystrixTimeOutHandler(@PathVariable("id") Integer id) {
        return "消费者Hystrix_8000,paymentHystrixTimeOut请求超时3秒,这是超时默认返回数据";
    }

    public String paymentHystrixTimeOutGlobalHandler() {
        return "全局异常处理降级返回";
    }
}