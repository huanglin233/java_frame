package com.hl.distributedlock.mysql;

import java.sql.Connection;

/**
 * 分布事实事物线程实例
 *
 * @author huanglin by 2021/5/19
 */
public class HandlerThread extends Thread{

    private final Long time;

    public HandlerThread(Long time) {
        this.time = time;
    }

    @Override
    public void run() {
        try {
//            doByPessimismLock(); //使排锁
//             doByTable(); // 使用记录锁
             doByTableVersion(); // 使用乐观锁
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 基于mysql悲观锁实现的分布式锁
      */
   private void doByPessimismLock() throws Exception{
       Mysql2Lock getLock = new Mysql2Lock();
       Connection connect = null;
       try {
           connect = getLock.connect();
           connect.setAutoCommit(false);
           System.out.println(Thread.currentThread().getName() + "-----开始获取锁-----");
           int i = getLock.getPessimismLock(connect);
           System.out.println(Thread.currentThread().getName() + "-----获取到锁 resource=> " + i + "-----");

           System.out.println(Thread.currentThread().getName() + "-----开始处理事物 do something-----");
           Thread.sleep(time);

           System.out.println(Thread.currentThread().getName() + "-----事物处理结束-----");

       }finally {
           System.out.println(Thread.currentThread().getName() + "-----释放锁-----");
           assert connect != null;
           connect.commit();
           connect.close();
       }
   }

    /**
     * 基于表记录实现的分布是锁，对一个条数据进行写入和删除
     */
    private void doByTable() throws Exception{
        Mysql2Lock getLock = new Mysql2Lock();
        Connection connect = null;

        try {
            connect = getLock.connect();
            System.out.println(Thread.currentThread().getName() + "-----开始获取锁-----");
            boolean lock = getLock.getLockByInsertInfo(connect);

            while(!lock) {
               lock = getLock.getLockByInsertInfo(connect);
            }

            System.out.println(Thread.currentThread().getName() + "-----获取到锁-----");

            System.out.println(Thread.currentThread().getName() + "-----开始处理事物 do something-----");
            Thread.sleep(time);

            System.out.println(Thread.currentThread().getName() + "-----事物处理结束-----");
        } finally {
            System.out.println(Thread.currentThread().getName() + "-----释放锁-----");
            assert connect != null;
            getLock.unLockByInsertInfo(connect);
            connect.close();
        }
    }

    /**
     * 基于乐观锁实现分布式锁
     */
    private void doByTableVersion() throws Exception{
        Mysql2Lock getLock = new Mysql2Lock();
        Connection connect = null;

        try {
            connect = getLock.connect();
            System.out.println(Thread.currentThread().getName() + "-----开始获取锁-----");
            LockBean lockByTableVersion = getLock.getLockByTableVersion(connect);

            System.out.println(Thread.currentThread().getName() + "-----获取到锁-----");

            System.out.println(Thread.currentThread().getName() + "-----开始处理事物 do something-----");
            Thread.sleep(time);

            System.out.println(Thread.currentThread().getName() + "-----事物处理结束-----");

            Boolean aBoolean = getLock.unlockByTableVersion(connect, lockByTableVersion.resource, lockByTableVersion.version);
            if(aBoolean) {
                System.out.println(Thread.currentThread().getName() + "------本次操作有效-----");
            } else {
                System.out.println(Thread.currentThread().getName() + "------本次操无效-----");
            }
        } finally {
            System.out.println(Thread.currentThread().getName() + "-----释放锁-----");
            assert connect != null;
            connect.close();
        }
    }
}