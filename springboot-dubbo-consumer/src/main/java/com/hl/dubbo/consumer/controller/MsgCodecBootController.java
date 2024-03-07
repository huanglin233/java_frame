package com.hl.dubbo.consumer.controller;

import org.apache.dubbo.config.annotation.Reference;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hl.dubbo.api.MsgCodecBoot;

@RestController
public class MsgCodecBootController {

    @Reference(version = "1.0L")
    MsgCodecBoot msgCodecBoot;

    @GetMapping("/getMsg")
    public String getMsg() {
        return msgCodecBoot.getMsg();
    }
}