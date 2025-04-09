package com.hl.bigdata.spark.scala.rdd

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, HBaseAdmin, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD

import java.util.Base64

/**
 * @author huanglin
 * @date 2022/04/09 16:40:02
 */
class Rdd_Hbase extends Rdd_BASE {

  val hbase_conf = HBaseConfiguration.create();

  def rdd_hbase_read(): Unit = {
    // hbase中表的名字
    hbase_conf.set(TableInputFormat.INPUT_TABLE, "test:t2");
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf1"))
    scan.setStartRow(Bytes.toBytes("row1"));
    scan.setStopRow(Bytes.toBytes("row100"));
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = new String(Base64.getEncoder.encode(proto.toByteArray))
    hbase_conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, scanToString)
    val hbase_rdd = sc.newAPIHadoopRDD(hbase_conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[Result]);
    hbase_rdd.cache();
    val count = hbase_rdd.count();
    println("hbase count : " + count);
    /*hbase_rdd.cache();
    hbase_rdd.foreach(println(_));*/
    hbase_rdd.foreach {
      case (_, result) => {
        val key = Bytes.toString(result.getRow);
        val name = Bytes.toString(result.getValue("cf1".getBytes(), "name".getBytes()));
        val age = Bytes.toInt(result.getValue("cf1".getBytes(), "age".getBytes()));
        println("key : " + key + " name" + " : " + name + " age" + " : " + age);
      }
    }

    sc.stop();
  }

  def rdd_hbase_insert(): Unit = {
    val hbase_job_conf = new JobConf(hbase_conf);
    hbase_job_conf.setOutputFormat(classOf[TableOutputFormat]);
    hbase_job_conf.set(TableOutputFormat.OUTPUT_TABLE, "ns1:t1");

    val table = TableName.valueOf("ns1:t1");
    val table_desc = new HTableDescriptor(table);
    table_desc.addFamily(new HColumnDescriptor("f1".getBytes()));

    val connection = ConnectionFactory.createConnection(hbase_conf);
    val admin = connection.getAdmin;
    if (admin.tableExists(table)) {
      admin.disableTable(table);
      admin.deleteTable(table);
    }
    admin.createTable(table_desc);

    def convert(triple: (Int, String, Int)) = {
      val put = new Put(Bytes.toBytes(triple._1));
      put.addImmutable(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(triple._2));
      put.addImmutable(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(triple._3));

      (new ImmutableBytesWritable, put);
    }

    val rdd_data: RDD[(Int, String, Int)] = sc.parallelize(List((1, "jim", 18), (2, "tom", 19), (3, "jack", 20), (4, "marry", 21)));
    val rdd_put: RDD[(ImmutableBytesWritable, Put)] = rdd_data.map(convert);
    rdd_put.saveAsHadoopDataset(hbase_job_conf);
  }
}

object Rdd_Hbase {

  def main(args: Array[String]): Unit = {
    new Rdd_Hbase().rdd_hbase_read();
    new Rdd_Hbase().rdd_hbase_insert();
  }
}
