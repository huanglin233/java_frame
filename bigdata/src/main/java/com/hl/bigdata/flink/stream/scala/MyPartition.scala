package com.hl.bigdata.flink.stream.scala

import org.apache.flink.api.common.functions.Partitioner

/**
 * @author huanglin
 * @date 2024/07/04 11:34
 */
class MyPartition extends Partitioner[Int]{

  override def partition(k: Int, i: Int): Int = {
    println("分区总数: " + i)
    if(k % 2 == 0) {
      0
    } else {
      1
    }
  }
}
