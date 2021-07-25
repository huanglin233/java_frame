package com.hl.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author huanglin
 * @date 2021/7/25 下午11:30
 */
public class TestSequenceFile {
    public static void main(String[] args) throws Exception {
        write();
    }

    /**
     * 写入序列文件
     */
    public static void write() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://s100:8020/");
        FileSystem          fs     = FileSystem.get(conf);
        Path                path   = new Path("hdfs://s100:8020/user/myseqfile.seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);

        IntWritable key = new IntWritable();
        Text        val = new Text();

        // 写入数据
        for(int i = 0; i < 100; i++) {
            key.set(i);
            val.set("tom" + i);
            writer.append(key, val);
        }

        System.out.println("write over");
    }
}
