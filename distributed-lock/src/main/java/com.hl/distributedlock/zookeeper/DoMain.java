package com.hl.distributedlock.zookeeper;

/**
 * zk分布式锁测试用例
 *
 * @author huanglin
 * @date 2021/5/29 下午6:01
 */
public class DoMain {

    public static void main(String[] args) throws InterruptedException {
        HandlerThread handler1 = new HandlerThread(1000L);
        HandlerThread handler2 = new HandlerThread(1000L);
        HandlerThread handler3 = new HandlerThread(1000L);
        HandlerThread handler4 = new HandlerThread(1000L);
        handler1.start();
        handler2.start();
        Thread.sleep(1000L);
        handler3.start();
        handler4.start();
    }
}
