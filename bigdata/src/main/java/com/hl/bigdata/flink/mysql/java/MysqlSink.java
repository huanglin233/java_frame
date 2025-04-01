package com.hl.bigdata.flink.mysql.java;

import com.hl.bigdata.flink.mysql.PageView;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * @author huanglin
 * @date 2025/04/01 20:49
 */
public class MysqlSink extends RichSinkFunction<List<PageView>> {

    PreparedStatement statement;
    BasicDataSource dataSource;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "INSERT INTO page_view(user_id, event_time, page_url, id) VALUES (?, ?, ?, ?)";
        statement = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(List<PageView> pvs, Context context) throws Exception {
        for (PageView pv : pvs) {
            statement.setLong(1, pv.getUserId());
            statement.setDate(2, new java.sql.Date(pv.getEventTime().getTime()));
            statement.setString(3, pv.getPageUrl());
            statement.setLong(4, pv.getId());
            statement.addBatch();
        }

        int[] size = statement.executeBatch();
        System.out.println("插入数据条数：" + size.length);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (statement != null) {
            statement.close();
        }
    }

    private static Connection getConnection(BasicDataSource dataSource) {

        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/flink_test?characterEncoding=utf8&useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("root");
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMaxIdle(2);
        Connection cn = null;
        try {
            cn = dataSource.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("数据库连接失败");
        }

        return cn;
    }
}
