package com.hl.cloudalibaba.nacosConfig.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RefreshScope  // 支持Nacos的动态刷新
public class NacosConfigController {

    @Value("${config.info}")
    private String configInfo;

    @GetMapping("/nacos/get/configInfo")
    public String getNacosConfigInfo() {
        return configInfo;
    }
}
