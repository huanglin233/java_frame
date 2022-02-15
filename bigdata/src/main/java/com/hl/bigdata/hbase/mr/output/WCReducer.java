package com.hl.bigdata.hbase.mr.output;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* * 
 * @Author: huanglin 
 * @Date: 2022-02-10 22:10:39 
 */
public class WCReducer extends TableReducer<Text, IntWritable, NullWritable> {
 
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value : values) {
            count = count + value.get();
        }

        Put put = new Put(Bytes.toBytes("row-" + key.toString()));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("word"), Bytes.toBytes(key.toString()));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("nums"), Bytes.toBytes(count));
        context.write(NullWritable.get(), put);
    }
}
