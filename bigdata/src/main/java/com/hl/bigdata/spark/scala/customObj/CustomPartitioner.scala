package com.hl.bigdata.spark.scala.customObj

import org.apache.spark.Partitioner

/**
 *  spark中自定义分区
 *
 * @author huanglin
 * @date 2022/03/19 17:13:31
 */
class CustomPartitioner extends Partitioner{

  // 覆盖分区数
  override def numPartitions: Int = 3
  // 覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int];
    val parNum = k % 2;
    println("CustomPartitioner.getPartition : " + key + " : " + parNum);

    parNum;
  }

  override def equals(other : Any) : Boolean = other match {
    case cp : CustomPartitioner => true
    case _ => false
  }
}
