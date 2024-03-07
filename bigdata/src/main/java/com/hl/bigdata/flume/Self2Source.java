package com.hl.bigdata.flume;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/* * 
 * @Author: huanglin 
 * @Date: 2022-02-27 19:39:26 
 */ 
public class Self2Source extends AbstractSource implements Configurable, PollableSource{
    
    @Override
    public synchronized void start() {
        // 初始化与外部客户端的连接
        super.start();
    }

    @Override
    public synchronized void stop() {
        // 释放资源或清空字段等
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        for(int i = 0; i < 10; i++) {
            Event e = new SimpleEvent();
            Map<String, String> headers = new HashMap<String, String>();
            headers.put("timestamp ", new Date().toString());
            headers.put("country", "china");
            e = new SimpleEvent();
            e.setBody((i + " hl").getBytes());
            e.setHeaders(headers);
            // 将数据发送到channel中
            getChannelProcessor().processEvent(e);
            status = Status.READY;
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void configure(Context context) {
        // 通过context获取配置文件中值
        
    }
}
