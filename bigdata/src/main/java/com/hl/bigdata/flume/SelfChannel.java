package com.hl.bigdata.flume;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.AbstractChannel;
import org.apache.flume.event.SimpleEvent;

/* * 
 * 自定义通道
 * @Author: huanglin 
 * @Date: 2022-03-01 20:37:26 
 */ 
public class SelfChannel extends AbstractChannel{

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            System.out.println("加载驱动失败");
        }
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public void put(Event event) throws ChannelException {
        Transaction transaction = getTransaction();
        try {
            transaction.begin();
            Connection        conn = DriverManager.getConnection("jdbc:mysql://192.168.0.183:3306/test", "root", "root");
            String            msg  = new String(event.getBody());
            PreparedStatement ps   = conn.prepareStatement("insert into t1(msg) values(?)");
            ps.setString(1, msg);
            ps.executeUpdate();
            ps.close();
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
        } finally {
            transaction.close();
        }
    }

    @Override
    public Event take() throws ChannelException {
        Transaction transaction = getTransaction();
        try{
            transaction.begin();
            Connection        conn = DriverManager.getConnection("jdbc:mysql://192.168.0.183:3306/test", "root", "root");
            conn.setAutoCommit(false);
            PreparedStatement ps   = conn.prepareStatement("select * from t1");
            ResultSet         rs   = ps.executeQuery();
            int               id   = 0;
            String            msg  = null;
            if(rs.next()) {
                id  = rs.getInt("id");
                msg = rs.getString("msg");
            }
            PreparedStatement ps2 = conn.prepareStatement("delete from t1 where id = ?");
            ps2.setInt(1, id);
            ps2.executeUpdate();
            conn.commit();

            rs.close();
            ps2.close();
            ps.close();
            conn.close();
            transaction.commit();
            SimpleEvent e = new SimpleEvent();
            e.setBody(msg.getBytes());

            return e;
        } catch (Exception e) {
            transaction.rollback();
        } finally {
            transaction.close();
        }
        
        return null;
    }

    @Override
    public Transaction getTransaction() {
        return new Transaction() {
            @Override
            public void begin() {
            }

            @Override
            public void commit() {
            }

            @Override
            public void rollback() {
            }

            @Override
            public void close() {
            }

        };
    }
    
}
