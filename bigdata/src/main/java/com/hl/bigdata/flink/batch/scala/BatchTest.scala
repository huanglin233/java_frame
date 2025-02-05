package com.hl.bigdata.flink.batch.scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.junit.Test

/**
 * flink 批处理
 *
 * @author huanglin
 * @date 2024/12/26 22:07
 */
class BatchTest {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  /**
   * 基于集合创建数据流
   */
  @Test
  def fromCollectionTest(): Unit = {
    // 1. 导入隐式转换
    import org.apache.flink.api.scala._

    val data = List(1, 2, 3, 4, 5)
    val text = env.fromCollection(data)
    val num = text.map(_ + 1)
    num.print().setParallelism(1)

    env.execute("fromCollectionTest")
  }

  /**
   * dataSet
   * Map：输入一个元素，然后返回一个元素，中间可以做一些清洗转换等操作
   * FlatMap：输入一个元素，可以返回零个，一个或者多个元素
   * MapPartition：类似map，一次处理一个分区的数据【如果在进行map处理的时候需要获取第三方资源链接，建议使用MapPartition】
   * Filter：过滤函数，对传入的数据进行判断，符合条件的数据会被留下
   * Reduce：对数据进行聚合操作，结合当前元素和上一次reduce返回的值进行聚合操作，然后返回一个新的值
   * Aggregate：sum、max、min等
   * Distinct：返回一个数据集中去重之后的元素，data.distinct()
   * Join：内连接
   * OuterJoin：外链接
   * Cross：获取两个数据集的笛卡尔积
   * Union：返回两个数据集的总和，数据类型需要一致
   * First-n：获取集合中的前N个元素
   * Sort Partition：在本地对数据集的所有分区进行排序，通过sortPartition()的链接调用来完成对多个字段的排序
   */

  /**
   * 广播变量
   */
  @Test
  def broadcastTest(): Unit = {
    // 1.准备广播数据
  }
}
