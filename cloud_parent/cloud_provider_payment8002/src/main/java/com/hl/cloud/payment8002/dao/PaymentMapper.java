package com.hl.cloud.payment8002.dao;

import org.apache.ibatis.annotations.Mapper;

import com.hl.cloud.entities.Payment;

@Mapper
public interface PaymentMapper {

    public Integer create(Payment payment);

    public Payment getPaymentById(Long id);
}