package com.hl.bigdata.hbase.mr.input;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* * 
 * 从hbase中读取数据
 * @Author: huanglin 
 * @Date: 2022-02-09 20:59:31 
 */
public class WCMapper extends TableMapper<Text, IntWritable>{
    
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String word = Bytes.toString(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
        context.write(new Text(word), new IntWritable(1));
    }
}
