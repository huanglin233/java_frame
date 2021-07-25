package com.hl.springbootKafka.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * kakfa stream 测试demo
 */
public class MyStream {

   public static void main(String[] args) {
       Properties properties = new Properties();
       // 程序的唯一标识符以区别于其他程序与同一kafka的通讯
       properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "hl");
       // 用于建立与kafka的初始连接的主机/端口的对应列表
       properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.56.101:9092");
       // 记录键值对的默认序列化和反序列化库
       properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
       properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
       // 定义steam应用程序的计算逻辑,计算逻辑定义为topology连接的处理器节点之一,构建流构建工具
       final StreamsBuilder builder = new StreamsBuilder();
       builder.stream("test").filter((k,v)->v.toString().split(",").length==2)
       .flatMap((k,v)->{
           List<KeyValue<String,String>> keyValues = new ArrayList<>();
           String[] info    = v.toString().split(",");
           String[] friends = info[1].split(" ");
           for (String friend:friends){
               keyValues.add(new KeyValue<String, String>(info[0].toString(),friend));
           }
           return keyValues;
       }).foreach(((k,v)-> System.out.println(k+"======="+v)));

       // 构建fopology对象
       final Topology topology = builder.build();
       // 打印算子钩子
       System.out.println(topology.describe().toString());
       // 构建kafka流API实例,将算子以及操作的服务器配置到kafka流中
       final KafkaStreams   streams = new KafkaStreams(topology, properties);
       final CountDownLatch latch   = new CountDownLatch(1);
       // 附加关闭处理程序来捕获
       Runtime.getRuntime().addShutdownHook(new Thread("hl") {
           @Override
           public void run() {
               streams.close();
               latch.countDown();
           }
       });

       try {
           streams.start();
           latch.await();
       } catch (InterruptedException e) {
        // 是非正常退出，就是说无论程序正在执行与否，都退出
        System.exit(1);
       }

       System.exit(0);
   }
}