package com.hl.cloudalibaba.sentinel.handler;

import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.hl.cloud.entities.Payment;
import com.hl.cloud.utils.CommonResult;

public class CustomerBlockHandler {

    public static CommonResult<Payment> handlerException(BlockException exception) {
        return new CommonResult<Payment>(444, "自定义 global handler exception-----1");
    }

    public static CommonResult<Payment> handlerException2(BlockException exception) {
        return new CommonResult<Payment>(444, "自定义 global handler exception-----2");
    }
}