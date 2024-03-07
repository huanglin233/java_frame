package com.hl.bigdata.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 处理数据和写入最终的结果
 * 
 * @author huanglin
 * @date 2021/08/04 21/51/00
 */
public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int maxVal = Integer.MIN_VALUE;

        // 找出最高温度
        for (IntWritable val : values) {
            maxVal = Math.max(maxVal, val.get());
        }

        // 写入输出
        context.write(key, new IntWritable(maxVal));
    }
}