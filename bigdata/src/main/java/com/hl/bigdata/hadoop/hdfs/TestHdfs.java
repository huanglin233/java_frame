package com.hl.bigdata.hadoop.hdfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestHdfs {

    static {
        // 注册协议处理器工厂,让java程序能够识别hdfs协议
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws Exception {
        // 定义url地址
        String url = "hdfs://s100:8020/user/hiveTest2.txt/000000_0";
        // url对象
        URL u = new URL(url);
        // url连接
        URLConnection openConnection = u.openConnection();
        // 打开输入流
        InputStream is = openConnection.getInputStream();
        // 出处流
        FileOutputStream fos = new FileOutputStream("/home/huanglin/bigdata/hiveTest.txt");
        byte[] buf = new byte[1024];
        int    len = -1;
        while((len = is.read(buf)) != -1) {
            fos.write(buf, 0, len);
        }
        
        is.close();
        fos.close();
        System.out.println("end");
    }

    // 测试向hdfs写入百万数据,供spark得job进行测试使用
    @Test
    public void insertData() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/spark/test2"));
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/spark/test/20220808.txt"));
        for(int i = 0; i < 3000000; i++) {
            StringBuilder sb = new StringBuilder();
            int id = (int) (Math.random() * 100);
            sb.append(id).append("\t").append(System.currentTimeMillis()).append("\t").append(UUID.randomUUID()).append("\t").append("text" + String.valueOf(i % 100)).append("\t").append("1").append("\t").append("1").append("\t").append("txt" + i).append("\n");
            fsDataOutputStream.write(sb.toString().getBytes());
            if(i % 500000 == 0) {
                fsDataOutputStream.flush();
            }
        }
        fsDataOutputStream.close();
    }


    @Test
    public void insertData2() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/spark/big_test1"));
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/spark/big_test1/20221210.txt"));
        for(int i = 0; i < 5000000; i++) {
            StringBuilder sb = new StringBuilder();
            int id = (int) (Math.random() * 100);
            // id value
            sb.append(id).append("\t").append(String.valueOf(i % 100)).append("\n");
            fsDataOutputStream.write(sb.toString().getBytes());
            if(i % 500000 == 0) {
                fsDataOutputStream.flush();
            }
        }
        fsDataOutputStream.close();
    }
    @Test
    public void insertData3() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/spark/small_test1"));
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/spark/small_test1/20221210.txt"));
        for(int i = 0; i < 1000000; i++) {
            StringBuilder sb = new StringBuilder();
            int id = (int) (Math.random() * 100);
            // id value
            sb.append(id).append("\t").append(String.valueOf(i % 100)).append("\n");
            fsDataOutputStream.write(sb.toString().getBytes());
            if(i % 500000 == 0) {
                fsDataOutputStream.flush();
            }
        }
        fsDataOutputStream.close();
    }

    @Test
    public void insertData4() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/spark/big_test1/20221210_3.txt"));
        for(int i = 0; i < 50000000; i++) {
            StringBuilder sb = new StringBuilder();
            int id = (int) (Math.random() * 100);
            // id value
            sb.append(i < 25000000 ? 33 : 55).append("\t").append(String.valueOf(i % 100)).append("\n");
            fsDataOutputStream.write(sb.toString().getBytes());
            if(i % 500000 == 0) {
                fsDataOutputStream.flush();
            }
        }
        fsDataOutputStream.close();
    }
}
