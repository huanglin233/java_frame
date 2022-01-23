package com.hl.bigdata.hadoop.mr.db.out;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 任务提交
 * 
 * @author huanglin
 * @date 2021/08/07 18/16/25
 */
public class OutApp {

    public static void main(String[] args) throws Exception {
        if(args.length != 1) {
            System.err.print("Usage: out db <inptut path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(OutApp.class);
        job.setJobName("out to db");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(DBOutputFormat.class);
        DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://172.17.0.1:3306/distributed_lock", "root", "root");
        DBOutputFormat.setOutput(job, "database_lock", "id", "version", "resource", "description");

        job.setMapperClass(OutDBMapper.class);
        job.setReducerClass(OutDBReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(OutDBWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 1 : 1);
    }
}