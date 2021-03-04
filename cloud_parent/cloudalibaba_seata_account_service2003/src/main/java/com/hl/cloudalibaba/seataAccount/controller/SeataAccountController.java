package com.hl.cloudalibaba.seataAccount.controller;

import java.math.BigDecimal;

import javax.annotation.Resource;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.hl.cloud.utils.CommonResult;
import com.hl.cloudalibaba.seataAccount.service.AccountServiceImpl;

@RestController
public class SeataAccountController {

    @Resource
    AccountServiceImpl accountServiceImpl;

    @SuppressWarnings("rawtypes")
    @RequestMapping("/account/decrease")
    public CommonResult decrease(@RequestParam("userId") Long userId, @RequestParam("money") BigDecimal money) {
        accountServiceImpl.decrease(userId, money);
        return new CommonResult(200, "扣减账户余额成功！");
    }
}