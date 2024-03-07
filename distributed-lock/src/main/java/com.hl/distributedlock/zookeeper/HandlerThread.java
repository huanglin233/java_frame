package com.hl.distributedlock.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 多线程使用zk分布式锁
 *
 * @author huanglin
 * @date 2021/5/29 下午5:03
 */
public class HandlerThread extends Thread{

    /**
     * zk分布式锁的根目录
     */
    private static final String ROOT = "/zk_lock";

    /**
     * zk分布式锁的根目录(Curator)
     */
    private static final String ROOT_CURATOR = "/zk_curator_lock";

    /**
     * zk分布式锁根目录下的锁资源节点
     */
    private static final String SOURCE = "lock";

    /**
     * 线程获取锁是否成功
     */
    private static boolean IS_GET_LOCK = true;

    private final Long sleepTime;

    public HandlerThread(Long time) {
        this.sleepTime = time;
    }

    @Override
    public void run() {
//        doByApacheZk();
//        doByZkClient();
        doByCurator();
    }

    /**
     * 通过apache zookeeper原生api连接zk,实现分布式锁
     */
    public void doByApacheZk() {
        ZooKeeper zk = null;
        // 使用信号量阻塞和唤醒获取锁和获取锁后的处理
        try {
            zk = Zk2Lock.getZookeeperByApache();
            // 创建临时节点锁
            String lockPath = zk.create(ROOT + "/" + SOURCE, SOURCE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName() + " ----- 已经创建临时锁 " + lockPath + "-----");
            // 获取目前的锁节点资源列表
            List<String> zkChildren = zk.getChildren(ROOT, false);
            zkChildren.sort(new LockSort());
            System.out.println(Thread.currentThread().getName() + "----- 目前的锁节点列表 " + zkChildren.toString() + "-----");
            // 判断当前创建的临时节点锁是否为最近的锁
            if((ROOT + "/" +zkChildren.get(0)).equals(lockPath)) {
                System.out.println(Thread.currentThread().getName() +"----- 获取锁成功,开始处理事情 ----");
                Thread.sleep(sleepTime);
            } else {
                int i = zkChildren.indexOf(lockPath.split(ROOT+"/")[1]);
                System.out.println(Thread.currentThread().getName() + "----- index = " + i + "-----");
                if(i > 0) {
                    String prePath = ROOT + "/" + zkChildren.get(i - 1);
                    System.out.println(Thread.currentThread().getName() +"----- " + lockPath + " 监听 " + prePath  + "----");
                    boolean lockState = watchPreNodeDel(Thread.currentThread().getName(), zk, lockPath, prePath);
                    if(lockState) {
                        Thread.sleep(sleepTime);
                    } else {
                        System.out.println(Thread.currentThread().getName() + "最终没有获取到锁,本线程处理事物失败");
                    }
                } else {
                    System.out.println(Thread.currentThread().getName() + "最终没有获取到锁,本线程处理事物失败");
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            assert zk != null;
            try {
                zk.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * 通过apache zookeeper原生zkclient api连接zk,实现分布式锁
     */
    public void doByZkClient() {
        ZkClient zkClient = null;
        try {
            zkClient = Zk2Lock.getZkClient();
            // 创建临时节点锁
            String lockPath = zkClient.createEphemeralSequential(ROOT + "/" + SOURCE, SOURCE.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            System.out.println(Thread.currentThread().getName() + " ----- 已经创建临时锁 " + lockPath + "-----");
            // 获取目前的锁节点资源列表
            List<String> zkChildren = zkClient.getChildren(ROOT);
            zkChildren.sort(new LockSort());
            System.out.println(Thread.currentThread().getName() + "----- 目前的锁节点列表 " + zkChildren.toString() + "-----");
            // 判断当前创建的临时节点锁是否为最近的锁
            if((ROOT + "/" +zkChildren.get(0)).equals(lockPath)) {
                System.out.println(Thread.currentThread().getName() +"----- 获取锁成功,开始处理事情 ----");
                Thread.sleep(sleepTime);
            } else {
                int i = zkChildren.indexOf(lockPath.split(ROOT+"/")[1]);
                System.out.println(Thread.currentThread().getName() + "----- index = " + i + "-----");
                if(i > 0) {
                    String prePath = ROOT + "/" + zkChildren.get(i - 1);
                    System.out.println(Thread.currentThread().getName() +"----- " + lockPath + " 监听 " + prePath  + "----");
                    boolean lockState = watchPreNodeDel(Thread.currentThread().getName(), zkClient, lockPath, prePath);
                    if(lockState) {
                        Thread.sleep(sleepTime);
                    } else {
                        System.out.println(Thread.currentThread().getName() + "最终没有获取到锁,本线程处理事物失败");
                    }
                } else {
                    System.out.println(Thread.currentThread().getName() + "最终没有获取到锁,本线程处理事物失败");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            assert zkClient != null;
            zkClient.close();
        }
    }

    /**
     * 通过Curator api实现分布式锁
     */
    public void doByCurator() {
        CuratorFramework  ctx   = null;
        InterProcessMutex mutex = null;
        try {
            ctx = Zk2Lock.getCuratorFramework();
            mutex = new InterProcessMutex(ctx, ROOT_CURATOR);
            if(mutex.acquire(1, TimeUnit.SECONDS)) {
                System.out.println(Thread.currentThread().getName() +"----- 获取锁成功,开始处理事情 ----");
                System.out.println(Thread.currentThread().getName() + "----- zkChildren " + ctx.getChildren().forPath(ROOT_CURATOR) + " -----");
                Thread.sleep(sleepTime);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            assert mutex != null;
            try {
                mutex.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 监听当前锁节点的前一个节点(通过Apache Zookeeper原生Api实现)
     * @param threadName  当前等待锁的线程名
     * @param zk          zk链接对象
     * @param lockPath    当前锁节点
     * @param preLockPath 前一个锁节点
     * @return
     * @throws InterruptedException
     */
    private boolean watchPreNodeDel(String threadName, ZooKeeper zk, String lockPath , String preLockPath) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        // 监听本节点前一个节点的删除
        try {
            zk.getData(preLockPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    Event.KeeperState state = event.getState();
                    Event.EventType   type  = event.getType();
                    if(Event.EventType.NodeDeleted == type && Event.KeeperState.SyncConnected == state) {
                        List<String> zkChildren = null;
                        try {
                            zkChildren = zk.getChildren(ROOT, false);
                            zkChildren.sort(new LockSort());
                            System.out.println(threadName + "----- 目前的锁节点列表 " + zkChildren.toString() + "-----");

                            // 前一个接节点已经删除, 在一次判断自己当前的节点锁是否为最近的
                            if((ROOT + "/" +zkChildren.get(0)).equals(lockPath)) {
                                System.out.println(threadName +"----- 获取锁成功,开始处理事情 ----");
                                IS_GET_LOCK = true;
                            } else {
                                int i = zkChildren.indexOf(lockPath.split(ROOT+"/")[1]);
                                if(i > 0) {
                                    String prePath = ROOT + "/" + zkChildren.get(i - 1);
                                    System.out.println(threadName + "----- " + lockPath + " 重新监听 " + prePath  + " ----");
                                    IS_GET_LOCK = watchPreNodeDel(threadName, zk, lockPath, prePath);
                                } else {
                                    IS_GET_LOCK = false;
                                }
                            }
                        } catch (KeeperException | InterruptedException e) {
                            IS_GET_LOCK = false;
                            e.printStackTrace();
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                }
            }, null);
        } catch (KeeperException | InterruptedException e) {
            IS_GET_LOCK = false;
            countDownLatch.countDown();
            e.printStackTrace();
        }

        countDownLatch.await();
        return IS_GET_LOCK;
    }

    /**
     * 监听当前锁节点的前一个节点(通过zkClient实现)
     * @param threadName
     * @param zkClient
     * @param lockPath
     * @param preLockPath
     * @return
     * @throws InterruptedException
     */
    private boolean watchPreNodeDel(String threadName, ZkClient zkClient, String lockPath, String preLockPath) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        // 监听本节点前一个节点的删除
        try {
            zkClient.subscribeDataChanges(preLockPath, new IZkDataListener() {
                @Override
                public void handleDataChange(String s, Object o) throws Exception {

                }

                @Override
                public void handleDataDeleted(String s) throws Exception {
                    try {
                        List<String> zkChildren = zkClient.getChildren(ROOT);
                        zkChildren.sort(new LockSort());
                        System.out.println(threadName + "----- 目前的锁节点列表 " + zkChildren.toString() + "-----");

                        // 前一个接节点已经删除, 在一次判断自己当前的节点锁是否为最近的
                        if((ROOT + "/" +zkChildren.get(0)).equals(lockPath)) {
                            System.out.println(threadName +"----- 获取锁成功,开始处理事情 ----");
                            IS_GET_LOCK = true;
                        } else {
                            int i = zkChildren.indexOf(lockPath.split(ROOT+"/")[1]);
                            if(i > 0) {
                                String prePath = ROOT + "/" + zkChildren.get(i - 1);
                                System.out.println(threadName + "----- " + lockPath + " 重新监听 " + prePath  + " ----");
                                IS_GET_LOCK = watchPreNodeDel(threadName, zkClient, lockPath, prePath);
                            } else {
                                IS_GET_LOCK = false;
                            }
                        }
                    } catch (Exception e) {
                        IS_GET_LOCK = false;
                        e.printStackTrace();
                    } finally{
                        countDownLatch.countDown();
                    }
                }
            });
        } catch (Exception e) {
            IS_GET_LOCK = false;
            countDownLatch.countDown();
            e.printStackTrace();
        }

        countDownLatch.await();
        return IS_GET_LOCK;
    }
}
