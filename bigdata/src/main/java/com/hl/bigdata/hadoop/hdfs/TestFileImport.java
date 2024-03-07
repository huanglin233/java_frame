package com.hl.bigdata.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * hdfs 文件导入
 * @author huanglin
 * @date 2021年11月16日 下午10:23:05
 *
 */
public class TestFileImport {

	public static void main(String[] args) {
		importFile2Hdfs("D:\\project\\java_frame\\bigdata\\src\\main\\resources\\people.txt", "/user/spark/people.txt");
	}
	
	private static void importFile2Hdfs(String localPath, String hdfsPath) {
		Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://s100:8020/");
        try {
			FileSystem fs = FileSystem.get(conf);
			FileInputStream fis = new FileInputStream(new File(localPath));
			OutputStream os = fs.create(new Path(hdfsPath));
			IOUtils.copyBytes(fis, os, 4096, true);
			fis.close();
			os.close();
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
