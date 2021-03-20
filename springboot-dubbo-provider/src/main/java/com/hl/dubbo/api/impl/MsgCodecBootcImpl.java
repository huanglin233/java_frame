package com.hl.dubbo.api.impl;

import org.apache.dubbo.config.annotation.Service;

import com.hl.dubbo.api.MsgCodecBoot;

@Service(version = "1.0L")
public class MsgCodecBootcImpl implements MsgCodecBoot{

    @Override
    public String getMsg() {
        // TODO Auto-generated method stub
        System.out.println("provider send msg");
        return "dubbo success";
    }
}