package com.hl.ssm.service.impl;

import org.apache.dubbo.config.annotation.Service;

import com.hl.ssm.service.MsgCodec;

@Service(version = "1.0L")
public class MsgCodecImpl implements MsgCodec{

    @Override
    public String getMsg() {
        // TODO Auto-generated method stub
        System.out.println("provider send msg");
        return "dubbo success";
    }
}