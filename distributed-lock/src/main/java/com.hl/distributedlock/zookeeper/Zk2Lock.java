package com.hl.distributedlock.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.*;

/**
 * zk分布式锁
 *
 * @author huanglin
 * @date 2021/5/29 下午1:48
 */
public class Zk2Lock {
    /**
     * zk地址
     */
    static final String ZK_ADDR = "192.168.56.101:2181";

    /**
     * 链接超时时间
     */
    static final int CONNECT_TIMEOUT = 2000;

    /**
     * 信号量，阻塞程序执行，用于等待zookeeper链接成功，发送信号
     */
    static final CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 基于apache zookeeper原生api获取zk链接对象
     * @return zk对象
     * @throws InterruptedException 线程中断异常
     * @throws IOException          链接异常
     */
    public static ZooKeeper getZookeeperByApache() throws InterruptedException, IOException {
        ZooKeeper zk = new ZooKeeper(ZK_ADDR, CONNECT_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 获取事件状态
                Event.KeeperState state = event.getState();
                Event.EventType   type  = event.getType();

                // 如果是建立链接
                if(Event.KeeperState.SyncConnected == state && Event.EventType.None == type) {
                    // 如果建立连接成功，发送信号量，让后续阻塞进行下去
                    countDownLatch.countDown();
                    System.out.println("---- zk 建立连接成功 -----");
                }
            }
        });

        countDownLatch.await();

        return zk;
    }

    /**
     * 基于ZkClinet获取zk连接对象
     * @return
     */
    public static ZkClient getZkClient() {
       return new ZkClient(ZK_ADDR, CONNECT_TIMEOUT);
    }

    /**
     * 基于CuratorFramework获取zk连接对象
     * @return
     */
    public static CuratorFramework getCuratorFramework() {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(ZK_ADDR, new RetryNTimes(10, CONNECT_TIMEOUT));
        curatorFramework.start();

        return curatorFramework;
    }

    public static void main(String[] args) {
        ZooKeeper zk = null;
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            zk = getZookeeperByApache();
            /**
             * 创建的节点类型
             * PERSISTENT （持久节点）
             * PERSISTENT_SEQUENTIAL（持久顺序节点）
             * EPHEMERAL（临时节点，本次会话有效）
             * EPHEMERAL_SEQUENTIAL（临时顺序节点，本次会话有效）
             */

            // 创建跟节点 第一次创建时需要
            zk.create("/zk_lock", "zk_lock".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            // 创建临时顺序节点
            String zkPath1 = zk.create("/zk_lock/lock", "lock".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            String zkPath2 = zk.create("/zk_lock/lock", "lock".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("----- zkPath1 " + zkPath1 + " -----");
            System.out.println("----- zkPath2 " + zkPath2 + " -----");
            // 获取节点信息
            List<String> zkChildren = zk.getChildren("/zk_lock", false);
            System.out.println("----- children size " + zkChildren.size() + " children : " + zkChildren.toString() + " ------");
            // zkPath2监听zkPath1节点的删除
            if(!zkChildren.get(zkChildren.size()-1).equals(zkPath2)) {
                zk.getData(zkPath1, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        Event.KeeperState state = event.getState();
                        Event.EventType   type  = event.getType();
                        System.out.println("----- state " + state + "-----");
                        if(Event.EventType.NodeDeleted == type && Event.KeeperState.SyncConnected == state) {
                            countDownLatch.countDown();
                        }
                    }
                }, null);
            }

            Thread.sleep(3000 * 2);
            zk.delete(zkPath1, -1);
            countDownLatch.await();
            System.out.println("前一个节点已经删除");
        } catch (InterruptedException | IOException | KeeperException e) {
            e.printStackTrace();
        } finally {
            assert zk != null;
            try {
                zk.close();
                System.out.println("----- zk closed -----");
            } catch (InterruptedException e) {
                System.out.println("---- zk 关闭连接失败 -----");
            }
        }
    }
}
