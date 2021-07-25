package com.hl.distributedlock.redis;

import org.redisson.api.RLock;

import java.util.concurrent.TimeUnit;

/**
 * 获取redis分布式锁处理事物
 * 
 * @author huanglin by 2021/5/20
 */
public class HandlerThread extends Thread{

    private static final String KEY = "redis-lock-key";

    private Long time;

    public HandlerThread(Long time) {
        this.time = time;
    }

    @Override
    public void run() {
        try {
//            doBySetnx(); // setnx的实现
//             doByNxPx(); // 基于nx + px + lua的实现
             doByRdisson(); // 基于redisson单机版实现
            // doByCRdisson(); // 基于redisson集群版实现
        } catch (Exception e) {
            System.out.println("获取锁失败");
            e.printStackTrace();
        }
    }

    /**
     * 通过redis处理分布式一， 使用redis的setnx进行
     * @throws Exception 
     */
    private void doBySetnx() throws Exception {
        Redis2Lock redis2Lock = new Redis2Lock();
        System.out.println(Thread.currentThread().getName() + "-----开始获取锁-----");
        long timeMillis = System.currentTimeMillis() + 1000 * 50;
        boolean lock = redis2Lock.lock(KEY, String.valueOf(timeMillis));
        if(lock) {
            System.out.println(Thread.currentThread().getName() + "-----已经获取到锁-----");
            // 处理业务逻辑(用睡眠代替)
            Thread.sleep(time);
            // 删除锁
            redis2Lock.unLock(KEY, String.valueOf(timeMillis));
        } else {
            System.out.println(Thread.currentThread().getName() + "-----获取锁失败-----");
        }
    }

    /**
     * 通过redis处理分布式一， 使用redis的nx px + lua脚本进行
     * @throws Exception
     */
    private void doByNxPx() throws Exception {
        Redis2Lock redis2Lock = new Redis2Lock();
        System.out.println(Thread.currentThread().getName() + "-----开始获取锁-----");
        long timeMillis = System.currentTimeMillis() + 1000 * 50;
        boolean lock = redis2Lock.lock2(KEY, String.valueOf(timeMillis));
        if(lock) {
            System.out.println(Thread.currentThread().getName() + "-----已经获取到锁-----");
            // 处理业务逻辑(用睡眠代替)
            Thread.sleep(time);
            // 删除锁
            redis2Lock.unLock2(KEY, String.valueOf(timeMillis));
        } else {
            System.out.println(Thread.currentThread().getName() + "-----获取锁失败-----");
        }
    }

    /**
     * 通过Ridesson实现分布锁(单机)
     * @throws Exception
     */
    private void doByRdisson() throws Exception {
        RLock rLock = Redis2Lock.getRLock();
        System.out.println(Thread.currentThread().getName() + "-----开始获取锁(单机模式)-----");
        try {
            boolean isLock = rLock.tryLock(5000, 15000, TimeUnit.MILLISECONDS);
            while(isLock) {
                System.out.println(Thread.currentThread().getName() + "-----已经获取到锁-----");
                // 处理业务逻辑(用睡眠代替)
                Thread.sleep(time);
                System.out.println(Thread.currentThread().getName() + "-----开始处理事务-----");
                isLock = false;
            }
        } finally {
            // 释放锁
            System.out.println(Thread.currentThread().getName() + "-----释放锁-----");
            rLock.unlock();
        }
    }

    /**
     * 通过Ridesson实现分布锁(集群)
     * @throws Exception
     */
    private void doByCRdisson() throws Exception {
        RLock rLock = Redis2Lock.getCRLock();
        System.out.println(Thread.currentThread().getName() + "-----开始获取锁(集群模式)-----");
        try {
            boolean isLock = rLock.tryLock(5000, 15000, TimeUnit.MILLISECONDS);
            if(isLock) {
                System.out.println(Thread.currentThread().getName() + "-----已经获取到锁-----");
                // 处理业务逻辑(用睡眠代替)
                Thread.sleep(time);
            }
        } finally {
            // 释放锁
            rLock.unlock();
        }
    }
}