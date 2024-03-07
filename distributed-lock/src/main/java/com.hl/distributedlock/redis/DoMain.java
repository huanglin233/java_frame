package com.hl.distributedlock.redis;

/**
 * 基于redis分布式锁的测试类
 * 
 * @author huanglin by 2021/5/20
 */
public class DoMain {

    public static void main(String[] args) {
        HandlerThread  hThread1 = new HandlerThread(100L);
        HandlerThread  hThread2 = new HandlerThread(200L);
        HandlerThread  hThread3 = new HandlerThread(10000L);

        hThread1.start();
        hThread2.start();
        hThread3.start();
    }
}