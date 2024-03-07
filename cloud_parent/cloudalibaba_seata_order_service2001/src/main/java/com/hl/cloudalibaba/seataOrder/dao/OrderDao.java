package com.hl.cloudalibaba.seataOrder.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.hl.cloudalibaba.seataOrder.domain.Order;

@Mapper
public interface OrderDao {

    /**
     * 1 新建订单
     * @param order
     * @return
     */
    int create(Order order);

    /**
     * 2 修改订单状态,从0改为1
     * @param userId
     * @param status
     * @return
     */
    int update(@Param("userId") Long userId, @Param("status") Integer status, @Param("id") Long id);
}