package com.hl.bigdata.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;
import org.junit.Test;

/* * 
 * API动态加载协处理器和卸载
 * @Author: huanglin 
 * @Date: 2022-01-30 17:15:51 
 */ 
public class LoadCoprocessor {
    
    private Connection    conn;
	private Configuration conf;
	private Admin         admin;

    /**
     * 初始连接
     * @throws Exception
     */
    @Before
    public void initConnection() throws Exception {
        conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    /**
     * 安装表中的所有协处理器
     * @throws Exception
     */
    @Test
    public void install() {
        try {
            // 先禁用表
            admin.disableTable(TableName.valueOf("test:t3"));
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf("test:t3"));
            table.addCoprocessor(AppendRegionObserver.class.getCanonicalName(), new Path("hdfs://s100:8020/hbase/bigdata-0.0.1-SNAPSHOT.jar"), 1, null);
            HColumnDescriptor column1 = new HColumnDescriptor("cf1");
            table.addFamily(column1);
            // 修改表
            admin.modifyTable(TableName.valueOf("test:t3"), table);
            // 重新启用表
            admin.enableTable(TableName.valueOf("test:t3"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 卸载表中的所有协处理器
     * @throws Exception
     */
    @Test
    public void unInstall(){
        try {
            // 先禁用表
            admin.disableTable(TableName.valueOf("test:t3"));
            HTableDescriptor  table   = new HTableDescriptor(TableName.valueOf("test:t3"));
            HColumnDescriptor column1 = new HColumnDescriptor("cf1");
            table.addFamily(column1);
            // 修改表
            admin.modifyTable(TableName.valueOf("test:t3"), table);
            // 重新启用表
            admin.enableTable(TableName.valueOf("test:t3"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
