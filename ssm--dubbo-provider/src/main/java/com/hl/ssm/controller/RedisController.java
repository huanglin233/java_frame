package com.hl.ssm.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hl.ssm.domain.User;
import com.hl.ssm.utils.RedisUtil;

@Controller
@RequestMapping(produces = { "text/html;charset=UTF-8;", "application/json;charset=UTF-8;" })
public class RedisController {

    @Autowired
    RedisUtil redisUtil;

    @GetMapping("/put/{key}")
    @ResponseBody
    public String putMsg(@PathVariable("key") String key) {
        User user = new User();
        user.setId(1);
        user.setAge(17);
        user.setSex("男");
        user.setName(key);

        boolean set = redisUtil.set(key, user);

        return set ? "success" : "faile";
    }

    @GetMapping("/get/{key}")
    @ResponseBody
    public String getMsg(@PathVariable("key") String key) {
        User user = (User)redisUtil.get(key);

        return user.toString();
    }
}