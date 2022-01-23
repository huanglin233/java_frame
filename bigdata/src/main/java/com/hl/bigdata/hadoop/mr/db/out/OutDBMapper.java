package com.hl.bigdata.hadoop.mr.db.out;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * mapper
 * 
 * @author huanglin
 * @date 2021/08/07 18/02/56
 */
public class OutDBMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
        String line = value.toString();
        String[] split = line.split(" ");
        for(String word : split) {
            context.write(new Text(word), new IntWritable(word.hashCode()));
        }
    };
}
