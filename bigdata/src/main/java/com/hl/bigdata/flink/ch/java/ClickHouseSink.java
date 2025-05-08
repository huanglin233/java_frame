package com.hl.bigdata.flink.ch.java;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Date;
import java.sql.PreparedStatement;

/**
 * clickhouse sink
 * @author huanglin
 * @date 2025/05/08 21:59
 */
public class ClickHouseSink extends RichSinkFunction<String> {

    private transient ClickHouseConnection connection;
    private transient PreparedStatement preparedStatement;
    private final String dbUrl;
    private final String user;
    private final String password;

    public ClickHouseSink(String dbUrl, String user, String password) {
        this.dbUrl = dbUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ClickHouseDataSource dataSource = new ClickHouseDataSource(dbUrl);
        if (user == null || password == null) {
            connection = dataSource.getConnection();
        } else {
            connection = dataSource.getConnection(user, password);
        }
        preparedStatement = connection.prepareStatement("insert into flink_table(id, name, event_time) values(?, ?, ?)");
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String[] split = value.split(",");
        preparedStatement.setInt(1, Integer.parseInt(split[0]));
        preparedStatement.setString(2, split[1]);
        preparedStatement.setDate(3, new Date(System.currentTimeMillis()));
//        preparedStatement.addBatch();
        preparedStatement.execute();
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
