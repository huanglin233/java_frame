package com.hl.bigdata.hadoop.dns;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.net.DNSToSwitchMapping;

public class MyDNSToSwitchMapping implements DNSToSwitchMapping{

    /**
     *  传递的是客户端的ip列表，返回机架感知的路径列表
     */
    @Override
    public List<String> resolve(List<String> names) {
        List<String> list = new ArrayList<String>();
        if(names != null && names.size() > 0) {
            for(String name : names) {
                // 主机名
                int ip = 0;
                if(name.startsWith("192")) {
                    ip = Integer.parseInt(name.substring(name.lastIndexOf(".") + 1));
                }
                
                if(ip < 103) {
                    list.add("/rack1" + ip);
                } else {
                    list.add("/rack2" + ip);
                }
            }
        }
        
        // 写入文件
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream("/home/huanglin/bigData/dns.txt");
            for(String name : list) {
                fos.write((name + "\r\n").getBytes());
        }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        return list;
    }

    @Override
    public void reloadCachedMappings() {
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
    }
}