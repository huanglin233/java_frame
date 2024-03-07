package com.hl.bigdata.hive.clineCRUD;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * hive-jdbc 操作hive
 * 
 * @author huanglin
 * @date 2021/08/10 22/45/30
 */
public class ClientCRUD {

    private static Connection conn;

    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://s100:10000/hi_tydic_loc", "APP", "mine");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
    	select2();
    }

    /**
     * hive创建表
     * @throws Exception
     */
    private static void create() throws Exception {
       PreparedStatement ps = conn.prepareStatement("create table employee2(id int, name string, age int)");
       ps.execute();
       ps.close();
       conn.close();
    }

    /**
     * hive插入数据
     * @throws Exception
     */
    private static void insert() throws Exception {
        PreparedStatement ps = conn.prepareStatement("insert into employee_par partition(province = ?, city = ?) values(?, ?, ?)");
        ps.setInt(3, 1);
        ps.setString(4, "tom");
        ps.setInt(5, 12);
        ps.setString(1, "sc");
        ps.setString(2, "bz");
        ps.executeUpdate();
        ps.close();
        conn.close();
    }

    /**
     * hive查询数据
     * @throws Exception
     */
    private static void select() throws Exception {
        PreparedStatement ps  = conn.prepareStatement("select id, time, value from employee2");
        ResultSet         ret = ps.executeQuery();
        while(ret.next()) {
            System.out.println("id = " + ret.getInt("id") + " ,time = " + ret.getString("time") + ", value = " + ret.getInt("value"));
        }
        ret.close();
        ps.close();
        conn.close();
    }

    private static void select2() throws Exception {
        PreparedStatement ps = conn.prepareStatement("select * from hi_tydic_loc.sk_int_user_loc_cell_flow_q limit 10");
        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()) {
            System.out.println("msisdn = " + resultSet.getString("msisdn"));
        }
        resultSet.close();
        ps.close();
        conn.close();
    }

    /**
     * hive函数使用
     */
    private static void count() throws Exception {
        PreparedStatement ps  = conn.prepareStatement("select count(*) from employee2");
        ResultSet         ret = ps.executeQuery();
        while(ret.next()) {
            int id = ret.getInt(1);
            System.out.println(id);
        }
        ret.close();
        ps.close();
        conn.close();
    }

    /**
     * hive 分区查询
     */
    private static void selectByPartition() throws Exception {
        PreparedStatement ps  = conn.prepareStatement("select * from employee_par where city = 'nj'");
        ResultSet         ret = ps.executeQuery();
        while(ret.next()) {
            int    id       = ret.getInt(1);
            String name     = ret.getString(2);
            int    age      = ret.getInt(3);
            String province = ret.getString(4);
            String city     = ret.getString(5);

            System.out.println(id + ": " + name + " " + age + " " + province + " " + city);
        }
        ret.close();
        ps.close();
        conn.close();
    }

    /**
     * hive删除记录
     */
    private static void delete() throws Exception {
        PreparedStatement ps = conn.prepareStatement("delete * from employee2");
        ps.executeUpdate();
        ps.close();
        conn.close();
    }

    /**
     * hive删除表 
     */
    private static void drop() throws Exception {
        PreparedStatement ps = conn.prepareStatement("drop table employee");
        ps.execute();
        ps.close();
        conn.close();
    }
}