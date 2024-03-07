package com.hl.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * 自定义分区函数
 * 
 * @author huanglin
 * @date 2021/08/05 21/42/44
 */
public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<Text, IntWritable>{

    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        return 0;
    }

}