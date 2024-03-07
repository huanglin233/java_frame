package com.hl.ssm.controller;

import org.apache.dubbo.config.annotation.Reference;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.hl.ssm.domain.User;
import com.hl.ssm.service.MsgCodec;
import com.hl.ssm.service.UserService;

@RestController
public class UserController {

    @Reference(version = "1.0L")
    UserService userService;

    @Reference(version = "1.0L")
    MsgCodec msgCodec;

    @GetMapping("/getUser/{id}")
    @ResponseBody
    public User getUser(@PathVariable("id") Long id) {
        User user = userService.getUser(id);
        System.out.println("-consumer-" + user.toString());
        return user;
    }

    @GetMapping("getMsg")
    @ResponseBody
    public String getMsg() {
        System.out.println("consumer get msg");
        return msgCodec.getMsg();
    } 
}