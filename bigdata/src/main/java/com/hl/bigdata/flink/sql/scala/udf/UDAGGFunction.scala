package com.hl.bigdata.flink.sql.scala.udf

import com.hl.bigdata.flink.sql.vo.AgeAvgAccum
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.functions.AggregateFunction

/**
 * @author huanglin
 * @date 2024/11/18 23:01
 */
@FunctionHint(output = new DataTypeHint("BIGINT"))
class UDAGGFunction extends AggregateFunction[Long, AgeAvgAccum] {

  override def getValue(acc: AgeAvgAccum): Long = {
    if (acc.count == 0) {
      0
    } else {
      acc.sum / acc.count
    }
  }

  override def createAccumulator(): AgeAvgAccum = {
    new AgeAvgAccum()
  }

  /**
   * 累加器 -- 每次更新数据时，更新聚合状态
   */
  def accumulate(acc: AgeAvgAccum, price: Long, num: Int): Unit = {
    acc.sum += price * num
    acc.count += 1
  }

  /**
   * 在撤销某条数据的影响时，更新当前的聚合状态
   */
  def retract(acc: AgeAvgAccum, price: Long, num: Int): Unit = {
    acc.sum -= price * num
    acc.count -= 1
  }

  /**
   * 合并两个聚合态
   */
  def merge(acc: AgeAvgAccum, its: Iterable[AgeAvgAccum]): Unit = {
    for(acc1 <- its) {
      acc.sum += acc1.sum
      acc.count += acc1.count
    }
  }

  /**
   * 重置累加器
   */
  def resetAccumulator(acc: AgeAvgAccum): Unit = {
    acc.count = 0
    acc.sum = 0
  }
}
