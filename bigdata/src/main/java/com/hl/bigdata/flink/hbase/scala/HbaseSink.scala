package com.hl.bigdata.flink.hbase.scala

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author huanglin
 * @date 2025/04/10 21:06
 */
class HbaseSink extends RichSinkFunction[String] {

  var con : Connection = _
  var table : Table = _

  override def open(parameters: Configuration): Unit = {
    val conf = HBaseConfiguration.create()
    con = ConnectionFactory.createConnection(conf)
    table = con.getTable(TableName.valueOf("test:t1"))
  }

  override def invoke(value: String, context: SinkFunction.Context): Unit = {
    try {
      val split = value.split(";")
      if (split.length != 4) {
        return
      }

      val put = new Put(Bytes.toBytes(split(0)))
      put.addColumn(Bytes.toBytes(split(1)), Bytes.toBytes(split(2)), Bytes.toBytes(split(3)))
      table.put(put)
    } catch {
      case e : Exception => {
        e.printStackTrace()
      }
    }
  }

  override def close(): Unit = {
    if (table != null) table.close()
    if (con != null) con.close()
  }
}
