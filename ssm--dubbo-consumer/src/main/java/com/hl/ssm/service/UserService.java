package com.hl.ssm.service;

import com.hl.ssm.domain.User;

public interface UserService{

    User getUser(Long id);

    String getMsg();
}