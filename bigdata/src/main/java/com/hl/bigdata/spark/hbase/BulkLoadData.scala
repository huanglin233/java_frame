package com.hl.bigdata.spark.hbase

import com.hl.bigdata.spark.scala.rdd.Rdd_BASE
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.KeyValue

/**
 * @author huanglin
 * @date 2023/02/15 22:28
 */
class BulkLoadData extends Rdd_BASE {

  val hbase_conf = HBaseConfiguration.create();
  val tableName = "test:t";
  hbase_conf.set(TableInputFormat.INPUT_TABLE, tableName);
  val table = new HTable(hbase_conf, tableName);

  def put(): Unit = {
    var p = new Put(new String("row1").getBytes());
    p.add("cf".getBytes(), "f1".getBytes(), new String("value1").getBytes());
    table.put(p);
    table.flushCommits();
  }

  def batchPut1(): Unit = {
    val job = Job.getInstance(hbase_conf);
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable]);
    job.setMapOutputValueClass(classOf[KeyValue]);
    HFileOutputFormat.configureIncrementalLoad(job, table);

    val num = sc.parallelize(11 to 20, 1);
    val rdd = num.map(x => {
      val kv: KeyValue = new KeyValue(Bytes.toBytes("row" + x), "cf".getBytes(), "f1".getBytes(), ("value" + x).getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })
    println(rdd.foreach(println));
    // hfiles on hdfs spark3.2写入hfile文件
    rdd.saveAsNewAPIHadoopFile("/tmp/t9", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], hbase_conf);
    //Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(hbase_conf)
    bulkLoader.doBulkLoad(new Path("/tmp/t9"), table)
  }

  def batchPut2() : Unit = {
    val job = Job.getInstance(hbase_conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat.configureIncrementalLoad(job, table)

    val num = sc.parallelize(11 to 20, 1)
    val rdd = num.map(x => {
      val kv: KeyValue = new KeyValue(Bytes.toBytes("row" + x), "cf".getBytes(), "f1".getBytes(), ("value" + x).getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })
    println(rdd.foreach(println));
    rdd.saveAsNewAPIHadoopFile("/tmp/t8", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], job.getConfiguration())
  }
}

  object BulkLoadData {

    def main(args: Array[String]): Unit = {
      val b = new BulkLoadData();
      b.batchPut1();
    }
}
