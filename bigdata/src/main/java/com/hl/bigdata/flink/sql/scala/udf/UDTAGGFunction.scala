package com.hl.bigdata.flink.sql.scala.udf

import com.hl.bigdata.flink.sql.vo.Top
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

/**
 * @author huanglin
 * @date 2024/11/19 22:55
 */
class UDTAGGFunction extends TableAggregateFunction[Tuple2[java.lang.Integer, java.lang.Integer], Top]{

  override def createAccumulator(): Top = {
    val top = new Top()
    top.first = Integer.MIN_VALUE
    top.second = Integer.MIN_VALUE
    top
  }

  def accumulate(acc: Top, v: java.lang.Integer): Unit = {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def merge(acc: Top, its: Iterable[Top]): Unit = {
    for (acc1 <- its) {
      accumulate(acc, acc1.first)
      accumulate(acc, acc1.second)
    }
  }

  def emitValue(acc: Top, out: Collector[Tuple2[java.lang.Integer, java.lang.Integer]]): Unit = {
    if (acc.first != Integer.MIN_VALUE) {
      out.collect(Tuple2.of(acc.first, 1))
    }

    if (acc.second != Integer.MIN_VALUE) {
      out.collect(Tuple2.of(acc.second, 2))
    }
  }
}
