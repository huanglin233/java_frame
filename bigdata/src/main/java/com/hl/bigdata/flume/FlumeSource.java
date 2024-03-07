package com.hl.bigdata.flume;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/* * 
 * flume数据源和配置文件
 * @Author: huanglin 
 * @Date: 2022-02-26 16:20:39 
 */ 
public class FlumeSource {
    
    /**
     * 上传flume配置文件到zk中
     * @throws Exception
     */
    @Test
    public void updateConfToZk() throws Exception {
        String    conn = "s100:2181";
        ZooKeeper zk   = new ZooKeeper(conn, 50000, new Watcher(){
            public void process(org.apache.zookeeper.WatchedEvent event) {
                System.out.println("over : " + event);
            };
        });
       Stat stat = new Stat();
       zk.create("/flume/a1", "first".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); // 持久节点
       zk.getData("/flume/a1", false, stat);
       int             v    = stat.getVersion();
       FileInputStream fis  = new FileInputStream("./src/main/java/com/hl/bigdata/flume/flume-conf.properties");
       byte[]          data = new byte[fis.available()];
       fis.read(data);
       fis.close();
       zk.setData("/flume/a1", data, v);
    }

    /**
     * 测试flume tcp source,发送TCP报文
     * @throws Exception
     */
    @Test
    public void sendMsgToTcp() throws Exception {
        Socket       socket       = new Socket( InetAddress.getByName("s100"), 8888);
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write(("hello world").getBytes());
        outputStream.flush();
        outputStream.close();
        System.out.println("over");
    }

    /**
     * 测试flume udp source,发送UDP报文
     * @throws Exception
     */
    @Test
    public void sendMsgToUdp() throws Exception {
        DatagramSocket socket = new DatagramSocket();
        byte[]         data   = "hello world".getBytes();
        DatagramPacket packet = new DatagramPacket(data, data.length);
        packet.setAddress(InetAddress.getByName("s100"));
        packet.setPort(8888);
        socket.send(packet);
        socket.close();
        System.out.println("over");
    }

    /**
     * 测试flume udp source,发送UDP报文
     * @throws Exception
     */
    @Test
    public void sendMsgHttp() throws Exception {
        URL               url        = new URL("http://s100:8888");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        OutputStream os = connection.getOutputStream();
        String json = "{\n"
        		+ "\"name\":\"jack\",\n"
        		+ "\"age\":16,\n"
        		+ "\"order_Id\": 001\n"
        		+ "}";
        os.write(json.getBytes());
        os.close();
        System.out.println("over");
    }
}