package com.hl.bigdata.hbase.mr.input;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* * 
 * @Author: huanglin 
 * @Date: 2022-02-09 21:06:11 
 */ 
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int count = 1;
        for(IntWritable value : values) {
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }
}
