package com.hl.bigdata.hbase.mr.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/* * 
 * @Author: huanglin 
 * @Date: 2022-02-10 22:26:05 
 */ 
public class WCApp {
    
    public static void main(String[] args) throws  Exception{
        Job           job  = Job.getInstance();
        Configuration conf = job.getConfiguration();
        conf.set("hbase.zookeeper.quorum", "s100:2181");
        job.setJarByClass(WCApp.class);
        job.setJobName("word count by hbase");

        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob("test:t4", scan, WCMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("test:t5", WCReducer.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
