package com.hl.bigdata.flink.stream.scala

import com.hl.bigdata.flink.stream.scala.source.{MySourceNonParallelism, MySourceParallelism}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.junit.Test

/**
 * @author huanglin
 * @date 2024/03/08 17:34
 */
class StreamingFromCollection {
  val env = StreamExecutionEnvironment.createLocalEnvironment()

  @Test
  def streamDemo(): Unit = {
    val data = List(10, 11, 12)

    val stream = env.fromCollection(data).map(_ + 1)
    stream.print()

    env.execute()
  }

  @Test
  def streamNonParallelismResource(): Unit = {
    val stream    = env.addSource(new MySourceNonParallelism).setParallelism(1)
    val mapStream = stream.map(x => {
      println("接收到数据: " + x)
      x
    })

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sum = mapStream.timeWindowAll(Time.seconds(5)).sum(0)
    sum.print()

    env.execute()
  }

  @Test
  def streamMultipleParallelism(): Unit = {
    val dataStream = env.addSource(new MySourceParallelism).setParallelism(3)
    val mapStream  = dataStream.map(x => {
      println("接收到数据: " + x)
      x
    })

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val sum = mapStream.timeWindowAll(Time.seconds(5)).sum(0);
    sum.print()

    env.execute()
  }

  @Test
  def filter(): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val dataStream = env.addSource(new MySourceNonParallelism).setParallelism(1)

    dataStream.map(x => {
      println("接收到数据: " + x)
      x
    }).filter(_%2 == 0).map(x => {
      println("过滤后数据: " + x)
      x
    }).timeWindowAll(Time.seconds(2)).sum(0).print();

    env.execute("filter")
  }

  @Test
  def split(): Unit = {
    val dataStream = env.addSource(new MySourceNonParallelism).setParallelism(1)

    val even: OutputTag[Int] = new OutputTag[Int]("even")
    val odd: OutputTag[Int]  = new OutputTag[Int]("odd")

    val processStream =  dataStream.process(new ProcessFunction[Int, Int] {
      override def processElement(i: Int, context: ProcessFunction[Int, Int]#Context, collector: Collector[Int]): Unit = {
        if(i%2 == 0) {
          context.output(even, i)
        } else {
          context.output(odd, i)
        }
      }
    })
    val evenStream = processStream.getSideOutput(even)
    val oddStream  = processStream.getSideOutput(odd)
    evenStream.print()

    env.execute("split")
  }

  @Test
  def union(): Unit = {
    val dataStream1 = env.addSource(new MySourceNonParallelism).setParallelism(1);
    val dataStream2 = env.addSource(new MySourceNonParallelism).setParallelism(1);

    val unionStream: DataStream[Int] = dataStream1.union(dataStream2);
    val mapStream = unionStream.map(new MapFunction[Int, Int] {
      override def map(t: Int): Int = {
        println("接收到的数据: " + t);

        return t;
      }
    })
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    mapStream.timeWindowAll(Time.seconds(2)).sum(0).print()

    env.execute("union")
  }

  @Test
  def connect(): Unit = {
    val dataStream1 = env.addSource(new MySourceNonParallelism).setParallelism(1)
    val dataStream2 = env.addSource(new MySourceNonParallelism).setParallelism(1)

    val mapStream = dataStream1.map("str" + _)
    mapStream.connect(dataStream2).map(new CoMapFunction[String, Int, Object] {
      override def map1(in1: String): Object = in1

      override def map2(in2: Int): Object = in2.toString
    }).print().setParallelism(1)

    env.execute("connect")
  }

  @Test
  def broadcast(): Unit = {
    env.setParallelism(4)
    val dataStream = env.addSource(new MySourceNonParallelism).setParallelism(1)
    val mapStream  = dataStream.broadcast.map(new MapFunction[Int, Int] {
      override def map(t: Int): Int = {
        val id  = Thread.currentThread().getId
        println("线程id: " + id + ", 接受到数据: " + t)

        return t;
      }
    })

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    mapStream.timeWindowAll(Time.seconds(2)).sum(0).print().setParallelism(1);

    env.execute("broadcast")
  }
}