package com.hl.cloudalibaba.seataOrder.controller;

import javax.annotation.Resource;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hl.cloud.utils.CommonResult;
import com.hl.cloudalibaba.seataOrder.domain.Order;
import com.hl.cloudalibaba.seataOrder.service.impl.OrderServiceImpl;

@RestController
public class OrderController {

    @Resource
    OrderServiceImpl orderServiceImpl;

    @SuppressWarnings("rawtypes")
    @GetMapping("/order/create")
    public CommonResult create(Order order) {
        orderServiceImpl.create(order);
        return new CommonResult(200, "订单创建成功");
    }
}