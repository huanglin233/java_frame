package com.hl.bigdata.spark.hbase

import com.hl.bigdata.spark.scala.rdd.Rdd_BASE
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author huanglin
 * @date 2023/02/18 15:59
 */
class Read_InputFormat extends Rdd_BASE{

/*
  hbase.mapreduce.inputtable
  hbase.mapreduce.splittable
  hbase.mapreduce.scan
  hbase.mapreduce.scan.row.start
  hbase.mapreduce.scan.row.stop
  hbase.mapreduce.scan.column.family
  hbase.mapreduce.scan.columns
  hbase.mapreduce.scan.timestamp
  hbase.mapreduce.scan.timerange.start
  hbase.mapreduce.scan.timerange.end
  hbase.mapreduce.scan.maxversions
  hbase.mapreduce.scan.cacheblocks
  hbase.mapreduce.scan.cachedrows
  hbase.mapreduce.scan.batchsize
  hbase.mapreduce.inputtable.shufflemaps
 */

  val hbase_conf = HBaseConfiguration.create();
  val tableName = "test:t";
  hbase_conf.set(TableInputFormat.INPUT_TABLE, tableName);

  def read1(): Unit = {
    hbase_conf.set(TableInputFormat.SCAN_ROW_START, "10");
    hbase_conf.set(TableInputFormat.SCAN_ROW_STOP, "10");
    val hbase_rdd = sc.newAPIHadoopRDD(hbase_conf, classOf[SaltRangeTableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]);

    hbase_rdd.foreach { case (_, result) =>
      val rowKey = Bytes.toString(result.getRow)
      val cell = result.listCells()
      cell.forEach(item => {
        val family = Bytes.toString(item.getFamilyArray, item.getFamilyOffset, item.getFamilyLength)
        val qualifier = Bytes.toString(item.getQualifierArray,
          item.getQualifierOffset, item.getQualifierLength)
        val value = Bytes.toString(item.getValueArray, item.getValueOffset, item.getValueLength)
        println(rowKey + " \t " + "column=" + family + ":" + qualifier + ", " +
          "timestamp=" + item.getTimestamp + ", value=" + value)
      });
    }
  }
}

object Read_InputFormat {

  def main(args: Array[String]): Unit = {
    new Read_InputFormat().read1();
  }
}