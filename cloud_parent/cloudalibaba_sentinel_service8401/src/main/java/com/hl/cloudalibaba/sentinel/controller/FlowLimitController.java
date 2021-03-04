package com.hl.cloudalibaba.sentinel.controller;

import java.util.Date;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.hl.cloud.entities.Payment;
import com.hl.cloud.utils.CommonResult;
import com.hl.cloudalibaba.sentinel.handler.CustomerBlockHandler;

@RestController
public class FlowLimitController {

    @GetMapping("/testA")
    public String testA() {
        System.out.println(new Date().getTime());
        return "-----testA";
    }

    @GetMapping("/testB")
    public String testB() {
        return "-----testB";
    }

    @GetMapping("/testC")
    public String testC() throws InterruptedException {
        Thread.sleep(10000);
        System.out.println("熔断,降级-----慢调用比例");

        return "-----testC";
    }

    @GetMapping("/testD")
    public String testD() {
        System.out.println("熔断,降级-----异常比例");
        int i = 10 / 0;

        return "-----testD i = " + i;
    }

    @GetMapping("/testE")
    public String testE() {
        System.out.println("熔断,降级-----异常数");
        int i = 10 / 0;

        return "-----testE i = " + i;
    }

    @GetMapping("/testHotKey")
    @SentinelResource(value = "testHotKey", blockHandler = "testHotKeyHandler", fallback = "testHotKeyHandler")
    public String testHotKey(@RequestParam(value = "p1",required = false) String p1,
            @RequestParam(value = "p2",required = false) String p2) {
        return "热点控制-----testHostKey, p1 = " + p1 + " p2 = " + p2;
    }
    public String testHotKeyHandler (String p1, String p2, BlockException exception) {
        return "it is testHotKey_handler,/(ㄒoㄒ)/~~";
    }

    @GetMapping("/byResource")
    @SentinelResource(value = "byResource", blockHandler = "handlerException")
    public CommonResult<Payment> byResouce() {
        Payment payment = new Payment();
        payment.setId(2021L);
        payment.setSerial("serial2021");
        return new CommonResult<Payment>(200, "按资源名称限流测试OK", payment);
    }
    public CommonResult<Payment> handlerException(BlockException exception) {
        return new CommonResult<Payment>(200, exception.getClass().getCanonicalName() + "\t 服务不可用");
    }

    @GetMapping("/customerBlockHandler")
    @SentinelResource(value = "customerBlockHandler", blockHandlerClass = CustomerBlockHandler.class, blockHandler = "handlerException2")
    public CommonResult<Payment> customerBlockHandler() {
        Payment payment = new Payment();
        payment.setId(2021L);
        payment.setSerial("serial2021");

        return new CommonResult<Payment>(200, "正常自定义消息", payment);
    }
}