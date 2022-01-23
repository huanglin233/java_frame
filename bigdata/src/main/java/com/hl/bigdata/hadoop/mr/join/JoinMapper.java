package com.hl.bigdata.hadoop.mr.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * join mapper
 * 
 * @author huanglin
 * @date 2021/08/08 15/47/04
 */
public class JoinMapper extends Mapper<LongWritable, Text, ComboKey, Text>{

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ComboKey, Text>.Context context) throws IOException, InterruptedException {
        int flag = 0;
        // 判断当前这个切片是Customer还是order
        FileSplit split = (FileSplit) context.getInputSplit();
        String    path  = split.getPath().toString();
        if(path.contains("customers")) {
            flag = 0;
        } else if(path.contains("orders")) {
            flag = 1;
        }

        int    cid   = 0;
        String line  = value.toString();
        String arr[] = line.split("\t");
        if(flag == 0) {
            cid = new Integer(arr[0]);
        } else if(flag == 1) {
            cid = new Integer(arr[3]); 
        }

        System.out.println(cid + " : " + flag + " : " + value);
        context.write(new ComboKey(cid, flag), value);
    }
}