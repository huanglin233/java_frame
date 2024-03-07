package com.hl.bigdata.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hl.bigdata.hadoop.mr.Util;

/**
 * Mapper
 * 
 * @author huanglin
 * @date 2021/08/05 21/44/55
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        context.getCounter("m", Util.getGrp2("WCMapper.setUp", this.hashCode())).increment(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String   line = value.toString();
        String[] arr  = line.split(",");
        for(String word : arr) {
            context.write(new Text(word), new IntWritable(1));
        }
        context.getCounter("m", Util.getGrp2("WCMapper.map", this.hashCode())).increment(1);
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        context.getCounter("m", Util.getGrp2("WCMapper.cleanup", this.hashCode())).increment(1);
    }
}