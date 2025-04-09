package com.hl.bigdata.flink.hbase.java;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author huanglin
 * @date 2025/04/07 21:49
 */
public class HbaseSink extends RichSinkFunction<String> {

    private Connection connection;
    private Table table;

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
        conf.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        conf.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf("test:t1"));
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        try {
            String[] split = value.split(";");
            if (split.length != 4) {
                return;
            }
            Put put = new Put(Bytes.toBytes(split[0]));
            put.addColumn(Bytes.toBytes(split[1]), // 列族
                    Bytes.toBytes(split[2]), // 列名
                    Bytes.toBytes(split[3])); // 列值
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if (table != null) table.close();
        if (connection != null) connection.close();
    }
}
