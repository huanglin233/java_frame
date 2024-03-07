package com.hl.bigdata.hadoop.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 提交作业
 * 
 * @author huanglin
 * @date 2021/08/04 21/59/25
 */
public class MaxTempApp {

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.out.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(MaxTempApp.class);

        // 设置作业名称
        job.setJobName("Max Handler App");
        // 设置输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置Mapper类型
        job.setMapperClass(MaxTempMapper.class);
        // 设置Reducer类型
        job.setReducerClass(MaxTempReducer.class);
        // 设置输出key类型
        job.setOutputKeyClass(Text.class);
        // 设置输出value类型
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}