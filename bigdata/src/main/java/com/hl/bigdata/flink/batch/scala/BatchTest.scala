package com.hl.bigdata.flink.batch.scala

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.{KeyedBroadcastProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration

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
    val broadcastData = List(("hl", 29), ("ll", 30), ("hh", 31))
    import org.apache.flink.api.scala.createTypeInformation
    val broadcastSet = env.fromCollection(broadcastData)

    // 2.处理广播数据
    val key = new MapStateDescriptor[String, Int]("broadcase", classOf[String], classOf[Int])
    val broadcast = broadcastSet.broadcast(key)

    // 3.源数据
    val data = env.fromElements("hl", "ll", "hh")
      .process(new ProcessFunction[String, String]() {

        override def processElement(i: String, context: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
          Thread.sleep(2000)
          collector.collect(i)
        }
      })

    data.keyBy(_.toString)
      .connect(broadcast)
      .process(new KeyedBroadcastProcessFunction[String, String, (String, Int), String] {

        override def processElement(in1: String, readOnlyContext: KeyedBroadcastProcessFunction[String, String, (String, Int), String]#ReadOnlyContext, collector: Collector[String]): Unit = {
          val broadcastState = readOnlyContext.getBroadcastState(key)
          val age = broadcastState.get(in1)
          collector.collect(s"key:$in1,age:$age")
        }

        override def processBroadcastElement(in2: (String, Int), context: KeyedBroadcastProcessFunction[String, String, (String, Int), String]#Context, collector: Collector[String]): Unit = {
          val broadcastState = context.getBroadcastState(key)
          broadcastState.put(in2._1, in2._2)
        }
      })
      .print()

    env.execute("broadcastTest")
  }

  /**
   * 累加器
   */
  @Test
  def counterTest(): Unit = {
    import org.apache.flink.api.scala.createTypeInformation
    val data = env.fromElements("a", "b", "c", "d", "e")
    data.map(new RichMapFunction[String, String] {

      // 1.创建累加器
      val counter = new IntCounter()

      override def open(parameter: Configuration): Unit = {
        // 2.注册累加器
        getRuntimeContext.addAccumulator("counter", counter)
      }

      override def map(in: String): String = {
        // 3.使用累加器
        counter.add(1)
        in
      }
    }).setParallelism(8)

    //4.获取累加器
    val re =  env.execute("counterTest")
    val num = re.getAccumulatorResult[Int]("counter")
    println(num)
  }

  /**
   * 关联两个流
   */
  @Test
  def joinTest(): Unit = {
    import org.apache.flink.api.scala.createTypeInformation

    val data1 = env.fromElements((1, "a"), (2, "b"), (3, "c"), (4, "d"), (1, "c"))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[(Int, String)] {
          override def extractTimestamp(element: (Int, String), recordTimestamp: Long): Long = 1
        }))

    val data2 = env.fromElements((1, 1), (2, 2), (3, 3), (1, 2))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(3))
          .withTimestampAssigner(new SerializableTimestampAssigner[(Int, Int)] {
            override def extractTimestamp(element: (Int, Int), recordTimestamp: Long): Long = 2
          }))
      
    data1.keyBy(_._1)
      .intervalJoin(data2.keyBy(_._1))
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new ProcessJoinFunction[(Int, String), (Int, Int), String]() {
        override def processElement(in1: (Int, String), in2: (Int, Int), context: ProcessJoinFunction[(Int, String), (Int, Int), String]#Context, collector: Collector[String]): Unit = {
          collector.collect(s"$in1-$in2")
        }
      }).print()

    env.execute("crossTest")
  }
}
