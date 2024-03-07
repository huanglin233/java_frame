package com.hl.bigdata.spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 * @author huanglin
 * @date 2023/02/15 20:40
 */
public class Java_Demo implements Serializable {

    String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    public void start() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("testJob");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("cf1"));
        scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"));

        try {
            String tableName = "test:t2";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            conf.set(TableInputFormat.SCAN, convertScanToString(scan));
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            JavaPairRDD<String, Integer> levels = hbaseRdd.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                    byte[] value = immutableBytesWritableResultTuple2._2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no"));
                    System.out.println(value);
                    if(value != null) {
                        return new Tuple2<String, Integer>(new String(value), 1);
                    }
                    return null;
                }
            });
            JavaPairRDD<String, Integer> counts = levels.reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });

            List<Tuple2<String, Integer>> output = counts.collect();
            for(Tuple2 tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }

            jsc.stop();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Java_Demo().start();
        System.exit(0);
    }
}
