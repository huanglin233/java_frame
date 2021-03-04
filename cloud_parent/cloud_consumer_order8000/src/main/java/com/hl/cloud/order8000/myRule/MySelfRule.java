package com.hl.cloud.order8000.myRule;

import org.springframework.context.annotation.Configuration;

import com.netflix.loadbalancer.IRule;
import com.netflix.loadbalancer.RandomRule;

@Configuration
public class MySelfRule {

    public IRule myRule() {
        return new RandomRule(); // 定义为随机
    }
}