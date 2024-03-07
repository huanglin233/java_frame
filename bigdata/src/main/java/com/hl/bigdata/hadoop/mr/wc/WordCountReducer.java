package com.hl.bigdata.hadoop.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.hl.bigdata.hadoop.mr.Util;

/**
 * reducer
 * 
 * @author huanglin
 * @date 2021/08/05 22/02/38
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)throws IOException, InterruptedException {
        context.getCounter("r", Util.getGrp2("WCReducer.setUp", this.hashCode())).increment(1);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable c : values) {
            count = count + c.get();
        }
        context.write(new Text(key), new IntWritable(count));
        context.getCounter("r", Util.getGrp2("WCReducer.reduce", this.hashCode())).increment(1);
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        context.getCounter("r", Util.getGrp2("WCReducer.cleanup", this.hashCode())).increment(1);
    }
}