package com.hl.distributedlock.mysql;

/**
 * 基于mysql的分布是锁的测试用例
 *
 * @author huanglin by 2021/5/19
 */
public class DoMain {

    public static void main(String[] args) {
        HandlerThread handler  = new HandlerThread(1000L * 5);
        HandlerThread handler2 = new HandlerThread(1000L * 2);
        HandlerThread handler3 = new HandlerThread(1000L * 10);

        handler.start();
        handler2.start();
        handler3.start();
    }
}