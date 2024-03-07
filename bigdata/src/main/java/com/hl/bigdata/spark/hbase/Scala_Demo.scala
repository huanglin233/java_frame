package com.hl.bigdata.spark.hbase

import com.hl.bigdata.spark.scala.rdd.Rdd_BASE
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

/**
 * @author huanglin
 * @date 2023/02/15 21:53
 */
class Scala_Demo extends Rdd_BASE{
  val hbase_conf = HBaseConfiguration.create();

  def readData() : Unit = {
    hbase_conf.set(TableInputFormat.INPUT_TABLE, "test:t2");
    val hbaseRdd = sc.newAPIHadoopRDD(hbase_conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]);

    val count = hbaseRdd.count();
    println(count)

    System.exit(0);
  }
}

object Scala_Demo {

  def main(args: Array[String]): Unit = {
    new Scala_Demo().readData();
  }
}
