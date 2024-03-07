package com.hl.bigdata.hadoop.mr.distributecache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * cache reducer
 * 
 * @author huanglin
 * @date 2021/08/08 13/45/24
 */
public class CacheReducer extends Reducer<IntWritable, Text, Text, NullWritable>{

    private Map<Integer, String> map = null;

    /**
     * 初始化 customer map 集合
     */
    @Override
    protected void setup(Reducer<IntWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        /**
         * 的到缓存uri
         */
        URI[]             uris = context.getCacheFiles();
        FSDataInputStream fis  = FileSystem.get(context.getConfiguration()).open(new Path(uris[0].getPath()));
        BufferedReader    br   = new BufferedReader(new InputStreamReader(fis));

        String line = null;
        map = new HashMap<Integer, String>();
        while((line = br.readLine()) != null) {
            map.put(new Integer(line.split("\t")[0]), line);
        }
        br.close();
        IOUtils.closeStream(fis);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 取得customerid
        int cid = key.get();
        for(Text order : values) {
            String info = map.get(cid);
            context.write(new Text(info + "\t" + order), NullWritable.get());
        }
    }
}