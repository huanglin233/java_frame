package com.hl.bigdata.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


/**
 * hive app
 * 
 * @author huanglin
 * @date 2021/08/08 17/08/14
 */
public class App {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://s100:10000/hive", "root", "root");
        PreparedStatement ps = conn.prepareStatement("select * from employee2");
        ResultSet query = ps.executeQuery();
        while(query.next()) {
            int    id   = query.getInt("id");
            String name = query.getString("name");
            int    age  = query.getInt("age");
            System.out.println(id + "," + name + "," + age);
        }
        query.close();
        ps.close();
        conn.close();
    }
}