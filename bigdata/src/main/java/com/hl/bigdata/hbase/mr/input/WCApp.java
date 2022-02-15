package com.hl.bigdata.hbase.mr.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* * 
 * @Author: huanglin 
 * @Date: 2022-02-09 21:09:11 
 */ 
public class WCApp {

    public static void main(String[] args) throws Exception {
        if(args.length != 1) {
            System.out.println("Usage: WCWord <output path>");
            System.exit(1);
        }

        Job           job  = Job.getInstance();
        Configuration conf = job.getConfiguration();
        conf.set("mapreduce.job.jar","bigdata-0.0.1-SNAPSHOT.jar");
        job.setJobName("word count by hbase");
        conf.set("hbase.zookeeper.quorum", "s100:2181");

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[0]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[0])); 
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob("test:t4", scan, WCMapper.class, Text.class, IntWritable.class, job);
        
        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "test:t4");
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
