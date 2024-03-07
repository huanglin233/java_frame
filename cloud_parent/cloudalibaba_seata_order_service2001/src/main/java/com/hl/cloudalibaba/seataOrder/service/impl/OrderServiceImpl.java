package com.hl.cloudalibaba.seataOrder.service.impl;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.hl.cloudalibaba.seataOrder.dao.OrderDao;
import com.hl.cloudalibaba.seataOrder.domain.Order;
import com.hl.cloudalibaba.seataOrder.service.AccountService;
import com.hl.cloudalibaba.seataOrder.service.OrderService;
import com.hl.cloudalibaba.seataOrder.service.StorageService;

import io.seata.spring.annotation.GlobalTransactional;

@Service
public class OrderServiceImpl implements OrderService{
    private static final Logger  logger= LoggerFactory.getLogger(OrderServiceImpl.class);

    @Resource
    OrderDao orderDao;
    @Resource
    AccountService accountService;
    @Resource
    StorageService storageService;

    /**
     * 创建订单->调用库存服务扣减库存->调用账户扣减金额->修改订单状态
     * GlobalTransactional seata开启分布式事务,异常时回滚,name保证唯一即可
     * @param order
     */
    @Override
    @GlobalTransactional(name = "fps-create-order", rollbackFor = Exception.class)
    public void create(Order order) {
        logger.info("-----开始创建订单");
        orderDao.create(order);

        logger.info("-----调用微服进行减库存");
        storageService.decrease(order.getProductId(), order.getCount());

        logger.info("-----调用微服进行扣款");
        accountService.decrease(order.getUserId(), order.getMoney());

        logger.info("-----修改订单状态");
        orderDao.update(order.getUserId(), 1, order.getId());

        logger.info("-----订单创建完成");
    }
}
