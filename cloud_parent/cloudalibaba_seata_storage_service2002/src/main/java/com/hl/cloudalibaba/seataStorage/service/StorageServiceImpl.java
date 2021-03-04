package com.hl.cloudalibaba.seataStorage.service;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.hl.cloudalibaba.seataStorage.dao.StorageDao;

@Service
public class StorageServiceImpl implements StorageService{
private static final Logger logger = LoggerFactory.getLogger(StorageServiceImpl.class);
    @Resource
    private StorageDao storageDao;
 
    // 扣减库存
    @Override
    public void decrease(Long productId, Integer count) {
        logger.info("-----storage-service中扣减库存开始");
        storageDao.decrease(productId,count);
        logger.info("-----storage-service中扣减库存结束");
    }
}