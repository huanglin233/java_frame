package com.hl.bigdata.hadoop.mr.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * join reducer
 * 
 * @author huanglin
 * @date 2021/08/08 15/55/02
 */
public class JoinReducer extends Reducer<ComboKey, Text, Text, NullWritable>{

    /**
     * reducer,每个分组调用一遍,key是变化的
     */
    @Override
    protected void reduce(ComboKey key, Iterable<Text> values, Reducer<ComboKey, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        String         cinfo    = iterator.next().toString();
        while(iterator.hasNext()) {
            System.out.println(key.hashCode());
            String oinfo = iterator.next().toString();
            context.write(new Text(cinfo + " " + oinfo), NullWritable.get());
        }
        context.write(new Text("over"), NullWritable.get());
    }
}