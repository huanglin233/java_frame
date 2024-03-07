package com.hl.cloudalibaba.seataStorage.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.hl.cloud.utils.CommonResult;
import com.hl.cloudalibaba.seataStorage.service.StorageService;

@RestController
public class SeataStorageController {

    @Autowired
    private StorageService storageService;

    //扣减库存
    @SuppressWarnings("rawtypes")
    @RequestMapping("/storage/decrease")
    public CommonResult decrease(@RequestParam("productId") Long productId, @RequestParam("count") Integer count) {
        storageService.decrease(productId, count);

        return new CommonResult(200,"扣减库存成功！");
    }
}