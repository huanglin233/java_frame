package com.hl.bigdata.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 提取和过滤需要处理的数据
 * 
 * @author huanglin
 * @date 2021/8/4 下午9:28
 */
public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行数据
       String line = value.toString();
       // 提取年份
       String year = line.substring(15, 19);
       // 气温
       int airTemperature;
       if(line.charAt(87) == '+') {
           airTemperature = Integer.parseInt(line.substring(88, 92));
       } else {
           airTemperature = Integer.parseInt(line.substring(87, 92));
       }

       // 质量
       String quality = line.substring(92, 93);

       // 判断气温的有效性
       if (airTemperature != MISSING && quality.matches("[01459]")) {
           context.write(new Text(year), new IntWritable(airTemperature));
       }
    }
}