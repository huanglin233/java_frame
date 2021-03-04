package com.hl.cloud.utils;

import java.io.Serializable;

public class CommonResult<T> implements Serializable{
    private static final long serialVersionUID = 1L;

    public Integer code;
    public String  message;
    public T       data;

    public CommonResult() {};

    public CommonResult(Integer code, String message, T data) {
        this.code    = code;
        this.message = message;
        this.data    = data;
    }

    public CommonResult(Integer code, String message) {
        this(code, message, null);
    }
}