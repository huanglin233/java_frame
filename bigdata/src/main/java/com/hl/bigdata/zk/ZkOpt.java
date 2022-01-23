package com.hl.bigdata.zk;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * 通过apache接口操作zk
 * @author huanglin
 * @date 2022年1月8日 下午7:23:03
 *
 */
public class ZkOpt {

	private ZooKeeper zk;
	private Watcher w;
	
	/**
	 * 创建zk节点
	 * @throws Exception 
	 */
	@Test
	public void createZNode() throws Exception {
		String con = "s100:2181";
		// 启用观察者
		zk = new ZooKeeper(con, 50000, new Watcher() {
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				System.out.println("event: " + event.getState() + " - " + event.getType() + "-" + event.getPath());
			}
		});
		
		System.out.println("开始创建节点");
		zk.create("/root", "first".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); // 持久节点
		System.out.println("创建完毕");
	}
	
	/**
	 * 创建临时有序节点
	 */
	@Test
	public void createSequenceZNode() {
		String con = "s100:2181";
		try {
			zk = new ZooKeeper(con, 50000, new Watcher() {
				public void process(WatchedEvent event) {
					System.out.println("event: " + event.getState() + " - " + event.getType() + "-" + event.getPath());
				}
			});
			System.out.println("开始创建临时节点");
			
			ACL       acl  = new ACL(Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE);
			List<ACL> acls = new ArrayList<ACL>();
			acls.add(acl);
			zk.create("/root/hl", "hello".getBytes(), acls, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("创建完毕");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 更新节点
	 */
	@Test
	public void updateZNode() {
		String con = "s100:2181";
		try {
			zk = new ZooKeeper(con, 50000, new Watcher() {
				public void process(WatchedEvent event) {
					System.out.println("event: " + event.getState() + " - " + event.getType() + "-" + event.getPath());
				}
			});
			zk.setData("/root", "firsroot".getBytes(), 0);

			System.out.println("更新完毕");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取节点的值
	 */
	@Test
	public void getZNode() {
		String con = "s100:2181";
		try {
			zk = new ZooKeeper(con, 50000, new Watcher() {
				public void process(WatchedEvent event) {
					System.out.println("event: " + event.getState() + " - " + event.getType() + "-" + event.getPath());
				}
			});
			Stat   stat = new Stat();
			byte[] data = zk.getData("/root", null, stat);
			System.out.println(new String(data, "UTF-8"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取节点的所有孩子节点
	 */
	@Test
	public void getChildrenZNode() {
		String con = "s100:2181";
		try {
			zk = new ZooKeeper(con, 50000, new Watcher() {
				public void process(WatchedEvent event) {
					System.out.println("event: " + event.getState() + " - " + event.getType() + "-" + event.getPath());
				}
			});
			List<String> children = zk.getChildren("/root", null);
			for(String s : children) {
				System.out.println(s);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 删除节点
	 */
	@Test
	public void deleteZNode() {
		String con = "s100:2181";
		ZooKeeper zk;
		try {
			zk = new ZooKeeper(con, 50000, new Watcher() {
				public void process(WatchedEvent event) {
					System.out.println("event: " + event.getState() + " - " + event.getType() + "-" + event.getPath());
				}
			});
			zk.delete("/root/hl0000000003", 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 观察节点变化
	 */
	@Test
	public void watcherZNode() {
		String con = "s100:2181";
		try {
			w = new Watcher() {
				public void process(WatchedEvent event) {
					System.out.println("有节点发生变化" + event);
					if(event.getType().equals(EventType.NodeDataChanged)) {
						try {
							zk.getData("/root", w, null);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			};
			zk = new ZooKeeper(con, 50000, w);
			byte[] data = zk.getData("/root", null, new Stat());
			System.out.println(new String(data, "UTF-8"));
			
			while(true) {
				Thread.sleep(3000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
