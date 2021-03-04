package com.hl.springbootRedis.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.hl.springbootRedis.utils.RedisUtil;

@RestController
public class RedisController {

    @Autowired
    RedisUtil redisUtil;

    @GetMapping("/add/{key}/{value}")
    public String add(@PathVariable("key") String key, @PathVariable("value") String value) {
        boolean set = redisUtil.set(key, value, 10000);

        return set ? "ok" : "faile";
    }

    @GetMapping("/get/{key}")
    public String get(@PathVariable("key") String key) {
        String value = (String) redisUtil.get(key);

        return value;
    }

    @GetMapping("/delete/{key}")
    public String delete(@PathVariable("key") String key) {
        redisUtil.del(key);

        return "delete ok";
    }

    @GetMapping("/expire/{key}/{time}")
    public String expire(@PathVariable("key") String key, @PathVariable("time") Long time) {
        boolean expire = redisUtil.expire(key, time);
 
        return expire ? "ok" : "faile";
    }
}