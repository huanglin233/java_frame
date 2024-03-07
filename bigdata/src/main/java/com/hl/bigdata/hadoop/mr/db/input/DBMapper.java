package com.hl.bigdata.hadoop.mr.db.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * db mapper
 * 
 * @author huanglin
 * @date 2021/08/07 15/51/41
 */
public class DBMapper extends Mapper<LongWritable, SysConfigDBWritable, Text, Text>{

    @Override
    protected void map(LongWritable key, SysConfigDBWritable value, Mapper<LongWritable, SysConfigDBWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        String variable = value.getVariable();
        String value1   = value.getValue();
        String setBy    = value.getSetBy();
        context.write(new Text(variable), new Text(value1 + " : " + setBy));
    }
}