package com.hl.distributedlock.mysql;


import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.*;

/**
 * 查询数据库获取锁
 *
 * @author huanglin by 2021/5/19
 */
public class Mysql2Lock {

    private static final String URL      = "jdbc:mysql://192.168.56.101:3306/distributed_lock";
    private static final String NAME     = "root";
    private static final String PASSWORD = "root";

    /**
     * jdbc链接
     */
    public Connection connect() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");

        return DriverManager.getConnection(URL, NAME, PASSWORD);
    }

    /**
     * c3p0链接
     */
    public Connection c3p0Connection() throws Exception {
        // 创建链接池对象
        ComboPooledDataSource pooledDataSource = new ComboPooledDataSource();
        pooledDataSource.setDriverClass("com.mysql.jdbc.Driver");
        pooledDataSource.setJdbcUrl(URL);
        pooledDataSource.setUser(NAME);
        pooledDataSource.setPassword(PASSWORD);

        // 设置最大链接数，默认15
        pooledDataSource.setMaxPoolSize(50);
        // 设置增量
        pooledDataSource.setAcquireIncrement(5);
        // 设置连接池最小链接数
        pooledDataSource.setMinPoolSize(2);
        // 链接初始化创建的链接数据，默认值3
        pooledDataSource.setInitialPoolSize(5);

        return pooledDataSource.getConnection();
    }

    /**
     * 分布式 => 悲观锁
     */
    public int getPessimismLock(Connection conn) throws Exception {
        Statement statement = conn.createStatement();

        String sql = "SELECT resource FROM database_lock where id = '1' FOR UPDATE";
        ResultSet resultSet = statement.executeQuery(sql);

        int i = -1;
        while(resultSet.next()) {
            i = resultSet.getInt("resource");
        }
        return i;
    }

    /**
     * 基于表记录的分布式锁， 获取锁
     */
    public boolean getLockByInsertInfo(Connection conn){

        try {
            Statement statement = conn.createStatement();

            String sql = "INSERT database_lock(id, resource, description) value(2, 1, '基于表记录的分布式锁')";
            statement.executeUpdate(sql);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    /**
     * 基于表记录的分布式锁， 删除锁
     */
    public boolean unLockByInsertInfo(Connection conn) throws Exception{
        Statement statement = conn.createStatement();

        String sql = "DELETE FROM database_lock where id = 2";
        int    i   = statement.executeUpdate(sql);

        return i == 1;
    }

    /**
     * 基于乐观锁实现分布是锁，获取锁资源
     */
    public LockBean getLockByTableVersion(Connection conn) throws Exception{
        Statement statement = conn.createStatement();

        String    sql       = "SELECT resource, version FROM database_lock where id = 3";
        ResultSet resultSet = statement.executeQuery(sql);

        LockBean lockBean = new LockBean();
        while(resultSet.next()) {
            lockBean.resource = resultSet.getInt("resource");
            lockBean.version  = resultSet.getInt("version");
        }

        return lockBean;
    }

    /**
     * 基于乐观锁实现分布式锁，更新资源版本
     */
    public Boolean unlockByTableVersion(Connection conn, Integer resource, Integer oldVersion) throws Exception{
        String            sql = "UPDATE database_lock SET resource = ?, version = ? WHERE id = ? AND version = ?";
        PreparedStatement ps  = conn.prepareStatement(sql);
        ps.setInt(1, resource -1);
        ps.setInt(2, oldVersion -1);
        ps.setInt(3, 3);
        ps.setInt(4, oldVersion);

        int i = ps.executeUpdate();

        return i == 1;
    }
}