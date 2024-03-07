package com.hl.cloud.streamRabbitmq.service;

import javax.annotation.Resource;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

import cn.hutool.core.lang.UUID;

@EnableBinding(Source.class)
public class MessageProviderImpl implements IMessageProvider{

    @Resource
    private MessageChannel output;

    @Override
    public String send() {
        String serial = UUID.randomUUID().toString();
        output.send(MessageBuilder.withPayload(serial).build());
        System.out.println("----serial: " + serial);

        return serial;
    }
}