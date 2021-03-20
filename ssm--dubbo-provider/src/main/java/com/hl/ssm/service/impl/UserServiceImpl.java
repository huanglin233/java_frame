package com.hl.ssm.service.impl;

import org.apache.dubbo.config.annotation.Service;
import org.springframework.beans.factory.annotation.Autowired;

import com.hl.ssm.domain.User;
import com.hl.ssm.mapper.UserMapper;
import com.hl.ssm.service.UserService;

@Service(version = "1.0L")
public class UserServiceImpl implements UserService{

    @Autowired
    UserMapper userMapper;

    public User getUser(Long id) {
        // TODO Auto-generated method stub
        System.out.println("------------------------");
        return userMapper.getUser(id);
    }

    @Override
    public String getMsg() {
        // TODO Auto-generated method stub
        return "dubbo success";
    }
}