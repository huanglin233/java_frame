package com.hl.bigdata.hadoop.hdfs;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

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
}
