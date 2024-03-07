package com.hl.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 任务提交
 * @author huanglin
 * @date 2021/08/05 22/11/53
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.out.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        // 删除已经存在输出文件
        Configuration configuration = job.getConfiguration();
        FileSystem    fileSystem    = FileSystem.get(configuration);
        fileSystem.delete(new Path(args[1]));

        job.setJarByClass(WordCountApp.class);
        job.setJobName("Word Count");

        // 设置输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 通过程序设置最小切片和最大切片
        // job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.maxsize", 15);
        // job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.minsize", 10);
        FileInputFormat.setMaxInputSplitSize(job, 15);
        FileInputFormat.setMinInputSplitSize(job, 10);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        configuration.setInt(MRJobConfig.NUM_MAPS, 5);
        job.setNumReduceTasks(4);

        // 设置分区函数
        job.setPartitionerClass(Partitioner.class);
        // 设置组合类,且必须是reducer
        job.setCombinerClass(WordCountReducer.class);

        // 开始执行job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}