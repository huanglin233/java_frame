package com.hl.springbootMinio.common;

import lombok.Data;

/**
 * 接口返回的数据封装类
 * @author huanglin
 * @date 2023/09/17 16:04
 */

@Data
public class Ret <T>{

    private static final Integer SUCCESS = 200;
    private static final Integer FAIL    = 400;

    private Integer code;
    private String  msg;
    private T       data;

    public Ret (T data, Integer code, String msg) {
        this.code = code;
        this.msg  = msg;
        this.data = data;
    }

    public Ret (T data, Integer code) {
        this.code = code;
        this.data = data;
    }

    public static<T> Ret<T> success(T data, String msg) {
        return new Ret<>(data, SUCCESS, msg);
    }

    public static<T> Ret<T> success(T data) {
        return new Ret<>(data, SUCCESS);
    }

    public static<T> Ret<T> fail(T data, String msg) {
        return new Ret<>(data, FAIL, msg);
    }

    public static<T> Ret<T> fail(String msg) {
        return new Ret<>(null, FAIL, msg);
    }
}
