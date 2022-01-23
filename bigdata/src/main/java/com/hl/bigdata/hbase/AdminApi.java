package com.hl.bigdata.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * admin接口提供了一系列可以获得集群访问状态的api的基本操作
 * 
 * @author huangling
 * @date 2022年1月23日 下午12:37:13
 */
public class AdminApi {
    
    private Connection conn;
    private Admin      admin;

    /**
     * 初始连接
     * @throws IOException
     */
    @Before
    public void initConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conn  = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    /**
     * 通过regionserver获取相关信息
     * @throws Exception
     */
    @Test
    public void getRegionServerInfo() throws Exception{
        ClusterStatus clusterStatus = admin.getClusterStatus(); // 返回一个ClusterStatus类，该类描述了集群整体的一些状态
        // 获取regionserver的数量
        int serversSize = clusterStatus.getServersSize();
        System.out.println("RegionServerSize: " + serversSize);
        // 获取挂掉了的regionserver数量
        int deadServers = clusterStatus.getDeadServers();
        System.out.println("DeadServers: " + deadServers);
        // 获取挂掉的regionserver名称
        Collection<ServerName> deadServerNames = clusterStatus.getDeadServerNames();
        for(ServerName serverName : deadServerNames) {
            System.out.println("DeadServerName: " + serverName.getServerName());
        }
        // 获取region的数量
        int regionsCount = clusterStatus.getRegionsCount();
        System.out.println("RegionsCount : " + regionsCount);
        // 获取平均负载
        double averageLoad = clusterStatus.getAverageLoad();
        System.out.println("AverageLoad: " + averageLoad);
        // 获取所有的循环出所有的regionserver信息
        for(ServerName serverName : clusterStatus.getServers()) {
            // 已知regionserver的名字，可以获取regionserver上的负载信息
            ServerLoad serverLoad = clusterStatus.getLoad(serverName);
            // 获取读请求计数
            long readRequestsCount = serverLoad.getReadRequestsCount();
            // 获取写请求计数
            long writeRequestsCount = serverLoad.getWriteRequestsCount();
            // 每秒请求数
            double requestsPerSecond = serverLoad.getRequestsPerSecond();
            System.out.println("ServerName: " + serverName + ", ReadRequestsCount: " + readRequestsCount + ", WriteRequestsCount: " + writeRequestsCount + ", RequestPerSecond: " + requestsPerSecond);
            // regionLoad与serverLoad类似，维护了region的一些负载信息
            for(Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                String key = Bytes.toString(entry.getKey());
                RegionLoad regionLoad = entry.getValue();
                System.out.println("ServerName: " + serverName + ", RegionName_" + key + ": " + regionLoad.toString());
                // 获取读请求计数
                System.out.println("ServerName: " + serverName + ", RegionName_" + regionLoad.getNameAsString() + ", ReadRequestsCount: " + regionLoad.getReadRequestsCount()
                                     + ", WriteRequestsCount: " + regionLoad.getWriteRequestsCount() + ", RequestCount: " + regionLoad.getRequestsCount());
            }
        }
    }
}
