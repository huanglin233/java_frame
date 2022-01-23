package com.hl.bigdata.hadoop.mr.join;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * join app
 * 
 * @author huanglin
 * @date 2021/08/08 16/01/59
 */
public class JoinApp {

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.err.print("Usage: MaxTemperatrue <input path> <output path>");
            System.exit(-1);
        }

        Job           job  = Job.getInstance();
        job.setJarByClass(JoinApp.class);
        job.setJobName("Join App");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setPartitionerClass(CIDPartitioner.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(ComboKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(CIDGroupComparator.class);
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}