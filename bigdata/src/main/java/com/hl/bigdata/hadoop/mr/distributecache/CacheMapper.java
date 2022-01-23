package com.hl.bigdata.hadoop.mr.distributecache;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * cache mapper
 * 
 * @author huanglin
 * @date 2021/08/08 13/39/13
 */
public class CacheMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        String   line      = value.toString();
        String[] orderInfo = line.split("\t");
        context.write(new IntWritable(Integer.parseInt(orderInfo[3])), value);
    }
}