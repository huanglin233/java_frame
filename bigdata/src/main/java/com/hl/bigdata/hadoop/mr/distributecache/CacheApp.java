package com.hl.bigdata.hadoop.mr.distributecache;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author huanglin
 * @date 2021/08/08 14/14/20
 */
public class CacheApp {

    public static void main(String[] args) throws Exception {
        if(args.length != 3) {
            System.err.print("Usage: MaxTemperature <input path> <output path> <customersidr>");
            System.exit(-1);
        }

        Job           job  = Job.getInstance();
        // 创建缓存文件
        job.setJobName("Cache Job");
        job.setJarByClass(CacheApp.class);
        URI uri = URI.create(args[2]);
        // 添加uri指定的路径资源到缓存中
        job.addCacheFile(uri);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CacheMapper.class);
        job.setReducerClass(CacheReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}