package com.hl.bigdata.spark.hbase

import com.hl.bigdata.spark.scala.rdd.Rdd_BASE
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.Job

/**
 * @author huanglin
 * @date 2023/02/15 22:28
 */
class BulkLoadData extends Rdd_BASE {

  val hbase_conf = HBaseConfiguration.create();
  val tableName = "test:t";
  hbase_conf.set(TableInputFormat.INPUT_TABLE, tableName);
  val connection = ConnectionFactory.createConnection(hbase_conf);
  val table = connection.getTable(TableName.valueOf(tableName))
  //  val table = new HTable(hbase_conf, tableName);

  def put(): Unit = {
    var p = new Put(new String("row1").getBytes());
    //    p.add("cf".getBytes(), "f1".getBytes(), new String("value1").getBytes());
    p.addColumn("cf".getBytes(), "f1".getBytes(), ("value1").getBytes())
    table.put(p);
    //    table.flushCommits();
  }

  def batchPut1(): Unit = {
    val job = Job.getInstance(hbase_conf);
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable]);
    job.setMapOutputValueClass(classOf[KeyValue]);
    //    HFileOutputFormat.configureIncrementalLoad(job, table);

    // 新增：获取RegionLocator
    val regionLocator = connection.getRegionLocator(TableName.valueOf(tableName))
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

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
    //    bulkLoader.doBulkLoad(new Path("/tmp/t9"), table)
    bulkLoader.doBulkLoad(new Path("/tmp/t9"), connection.getAdmin, table, regionLocator)
  }

  def batchPut2(): Unit = {
    val job = Job.getInstance(hbase_conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    //    HFileOutputFormat.configureIncrementalLoad(job, table)
    val regionLocator = connection.getRegionLocator(TableName.valueOf(tableName))
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

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
