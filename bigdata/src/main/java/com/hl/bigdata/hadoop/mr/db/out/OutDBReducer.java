package com.hl.bigdata.hadoop.mr.db.out;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * outinput reducer
 * 
 * @author huanglin
 * @date 2021/08/07 18/07/29
 */
public class OutDBReducer extends Reducer<Text, IntWritable, OutDBWritable, NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, OutDBWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        Long count = 0l;
        for(IntWritable id : values) {
            count += id.get();
        }

        OutDBWritable out = new OutDBWritable();
        out.setId(count);
        out.setVersion(1);
        out.setResource(1);
        out.setDescription(key.toString());

        context.write(out, NullWritable.get());
    }
}