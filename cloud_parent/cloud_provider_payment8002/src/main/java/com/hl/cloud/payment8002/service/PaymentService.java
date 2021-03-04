package com.hl.cloud.payment8002.service;

import com.hl.cloud.entities.Payment;

public interface PaymentService {

    /**
     * 创建一条payment数据
     * @param  payment
     * @return
     */
    public int create(Payment payment);

    /**
     * 根据payment id查找响应数据
     * @param  id
     * @return
     */
    public Payment getPaymentById(Long id);
}