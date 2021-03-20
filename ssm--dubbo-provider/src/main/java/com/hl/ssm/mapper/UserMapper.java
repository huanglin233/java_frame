package com.hl.ssm.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.hl.ssm.domain.User;

@Mapper
public interface UserMapper {

    User getUser(@Param("id") Long id);
}