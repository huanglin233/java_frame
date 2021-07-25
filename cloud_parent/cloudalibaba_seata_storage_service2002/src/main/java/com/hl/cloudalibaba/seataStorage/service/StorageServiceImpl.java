package com.hl.cloudalibaba.seataStorage.service;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.hl.cloudalibaba.seataStorage.dao.StorageDao;

@Service
@Transactional
public class StorageServiceImpl implements StorageService{
    private static final Logger logger = LoggerFactory.getLogger(StorageServiceImpl.class);

    @Resource
    private StorageDao storageDao;
 
    // 扣减库存
    @Override
    public void decrease(Long productId, Integer count) {
        logger.info("-----storage-service中扣减库存开始");
        int decrease = storageDao.decrease(productId,count);
        if(decrease == 0) {
            logger.info("decrease = {}减库失败，抛出异常", decrease);
            throw new RuntimeException("减库失败");
        }
        logger.info("-----storage-service中扣减库存结束");
    }
}