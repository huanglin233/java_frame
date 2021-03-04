package com.hl.cloudalibaba.seataOrder.service;

import java.math.BigDecimal;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.hl.cloud.utils.CommonResult;

@FeignClient(value = "seata-account-service")
public interface AccountService {

    /**
     * 减余额
     * @param userId
     * @param money
     * @return
     */
    @SuppressWarnings("rawtypes")
    @PostMapping(value = "/account/decrease")
    CommonResult decrease(@RequestParam("userId") Long userId, @RequestParam("money") BigDecimal money);
}
