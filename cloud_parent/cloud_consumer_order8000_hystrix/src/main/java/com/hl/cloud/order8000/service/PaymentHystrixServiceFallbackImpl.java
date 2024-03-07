package com.hl.cloud.order8000.service;

import org.springframework.stereotype.Component;

@Component
public class PaymentHystrixServiceFallbackImpl implements PaymentHystirxService{

    @Override
    public String paymentInfo_ok(Integer id) {
        return "-----paymentInfo_ok fallback-----";
    }

    @Override
    public String paymentInfoTimeOut(Integer id) {
        return "-----paymentInfoTimeOut fallback-----";
    }

}