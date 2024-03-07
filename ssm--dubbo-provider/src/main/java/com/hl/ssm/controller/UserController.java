package com.hl.ssm.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hl.ssm.service.UserService;

@Controller
@RequestMapping(produces = { "text/html;charset=UTF-8;", "application/json;charset=UTF-8;" })
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping("/getUser/{id}")
    @ResponseBody
    public String getUserById(@PathVariable("id") Long id) {
        String string = userService.getUser(id).toString();
        System.out.println("result-- " + string);
        return string;
    }

    @RequestMapping(value = "/getMsg", method = RequestMethod.GET)
    @ResponseBody
    public String getMsg() {
        return "msg ok 爱仕达";
    }
}