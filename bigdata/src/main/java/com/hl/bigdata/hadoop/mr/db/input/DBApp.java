package com.hl.bigdata.hadoop.mr.db.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * db 任务提交
 * 
 * @author huanglin
 * @date 2021/08/07 15/57/27
 */
public class DBApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 1) {
            System.out.println("Usage: DBInput <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        // 设置配置参数
        Configuration configuration = job.getConfiguration();
        job.setJarByClass(DBApp.class);
        job.setJobName("DB input App");
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.setInputFormatClass(DBInputFormat.class);
        // 设置数据库配置
        DBConfiguration.configureDB(configuration, "com.mysql.jdbc.Driver", "jdbc:mysql://172.17.0.1:3306/sys", "root", "root");
        // 设置输入
        DBInputFormat.setInput(job, SysConfigDBWritable.class, "select sc.variable as variable, sc.value as value, sc.set_time as setTime, sc.set_by as setBy from sys_config as sc", "select count(*) from sys_config");
        // 设置mapper
        job.setMapperClass(DBMapper.class);
        // 设置输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}