package com.hl.distributedlock.mysql;

/**
 * @author huanglin by 2021/5/20
 */
public class LockBean {
    public Long    id;
    public Integer version;
    public Integer resource;
    public String  description;
}