package com.hl.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.Test;

import java.io.IOException;

/**
 * @author huanglin
 * @date 2021/7/25 下午11:30
 */
public class TestSequenceFile {
    public static void main(String[] args) throws Exception {
        readSeql();
    }

    /**
     * 写入序列文件
     */
    public static void write() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s100:8020/user/myseqfile.seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);

        IntWritable key = new IntWritable();
        Text val = new Text();

        // 写入数据
        for (int i = 0; i < 100; i++) {
            key.set(i);
            val.set("tom_jack" + i);
            writer.append(key, val);
        }

        System.out.println("write over");
        writer.close();
    }

    /**
     * 写入sequFile,手动主动同步点
     * SequenceFile key--value 类似于map
     */
    public static void writeWithSync() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s100:8020/user/myseqfile2.seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);

        IntWritable key = new IntWritable();
        Text value = new Text();

        // 写入数据
        for(int i = 0; i < 100; i++) {
            key.set(i);
            value.set("Jack" + i);
            writer.append(key, value);
            if(i % 5 == 0) {
                writer.sync();
            }
        }

        System.out.println("write over");
        writer.close();
    }

    public static void readSeql() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s100:8020/user/myseqfile.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable key = new IntWritable();
        Text value = new Text();

        while(reader.next(key, value)) {
            System.out.println(key + " = " + value);
        }

        System.out.println("read over");
        reader.close();;
    }

    public static void getSeqFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s100:8020/user/myseqfile2.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable key = new IntWritable();
        Text value = new Text();
        Class<?> keyClass = reader.getKeyClass();
        Class<?> valueClass = reader.getValueClass();

        // 获取压缩类型
        SequenceFile.CompressionType compressionType = reader.getCompressionType();
        // 压缩编解码器
        CompressionCodec compressionCodec = reader.getCompressionCodec();

        // 得到当前key对应的字节数
        long position = reader.getPosition();
        System.out.println(position);

        while(reader.next(key, value)) {
            System.out.println(position + " : " + key + " = " + value);
            position = reader.getPosition();
        }

        System.out.println("read over");
        reader.close();
    }

    /**
     * 读取seqfile文件指定位置内容
     */
    public static void seekSeq() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s100:8020/user/myseqfile2.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable key = new IntWritable();
        Text value = new Text();
        // 定位指针到指定的位置
        reader.seek(252);
        reader.next(key, value);

        System.out.println(key + " = " + value);
        reader.close();
    }

    /**
     * 读取seqfile同步点
     */
    public static void sync() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://s100:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s100:8020/user/myseqfile2.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable key = new IntWritable();
        Text value = new Text();
        int syncPos = 304;
        // 定义下一个同步点
        reader.sync(syncPos);
        // 的到当前的指针位置
        long pos = reader.getPosition();
        reader.next(key, value);
        System.out.println(syncPos + " : " + pos + " : " + key + " = " +value);

        reader.close();
    }
}