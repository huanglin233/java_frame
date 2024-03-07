package com.hl.cloud.payment8005.service;

import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

@Service
public class PaymentService {

    public String paymentInfo_ok(Integer id) {
        return "线程池 : " + Thread.currentThread().getName() + "   paymentInfo_ok,id : " + id + "\t" + "O(∩_∩)O哈哈~";
    }


    public String paymentInfo_TimeOut(Integer id) {
        int timeOut = 5;
        try {
            TimeUnit.SECONDS.sleep(timeOut);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "线程池 : " + Thread.currentThread().getName() + "   paymentInfo_ok,id : " + id + "\t" + "~~~~(>_<)~~~~,耗时(秒)" + timeOut;
    }
}