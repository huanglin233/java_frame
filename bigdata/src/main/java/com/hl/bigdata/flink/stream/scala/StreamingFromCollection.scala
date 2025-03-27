package com.hl.bigdata.flink.stream.scala

import com.hl.bigdata.flink.custom.scala.{CustomWindowAssigner}
import com.hl.bigdata.flink.stream.scala.source.{MySourceNonParallelism, MySourceParallelism, MySourceWorld}
import groovy.util.logging.Slf4j
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector
import org.junit.Test

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

/**
 * @author huanglin
 * @date 2024/03/08 17:34
 */
@Slf4j
class StreamingFromCollection {
  val conf: Configuration = new Configuration();
  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

  @Test
  def streamDemo(): Unit = {
    val data = List(10, 11, 12)

    val stream = env.fromCollection(data).map(_ + 1)
    stream.print()

    env.execute()
  }

  @Test
  def streamNonParallelismResource(): Unit = {
    val stream = env.addSource(new MySourceNonParallelism).setParallelism(1)
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
    val mapStream = dataStream.map(x => {
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
    }).filter(_ % 2 == 0).map(x => {
      println("过滤后数据: " + x)
      x
    }).timeWindowAll(Time.seconds(2)).sum(0).print();

    env.execute("filter")
  }

  @Test
  def split(): Unit = {
    val dataStream = env.addSource(new MySourceNonParallelism).setParallelism(1)

    val even: OutputTag[Int] = new OutputTag[Int]("even")
    val odd: OutputTag[Int] = new OutputTag[Int]("odd")

    val processStream = dataStream.process(new ProcessFunction[Int, Int] {
      override def processElement(i: Int, context: ProcessFunction[Int, Int]#Context, collector: Collector[Int]): Unit = {
        if (i % 2 == 0) {
          context.output(even, i)
        } else {
          context.output(odd, i)
        }
      }
    })
    val evenStream = processStream.getSideOutput(even)
    val oddStream = processStream.getSideOutput(odd)
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
    val mapStream = dataStream.broadcast.map(new MapFunction[Int, Int] {
      override def map(t: Int): Int = {
        val id = Thread.currentThread().getId
        println("线程id: " + id + ", 接受到数据: " + t)

        return t;
      }
    })

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    mapStream.timeWindowAll(Time.seconds(2)).sum(0).print().setParallelism(1);

    env.execute("broadcast")
  }

  @Test
  def checkPoint(): Unit = {
    env.enableCheckpointing(1000);
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500);
    env.getCheckpointConfig.setCheckpointTimeout(600);
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val dataStream = env.addSource(new MySourceWorld).setParallelism(1)
    dataStream.flatMap(_.split("\\s"))
      .map((_, 1))
      .keyBy(e => e._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .reduce((a, b) => {
        (a._1, a._2 + b._2)
      })
      .print()

    env.execute("source window count")
  }

  @Test
  def myParition(): Unit = {
    env.setParallelism(1);
    val dataStream = env.addSource(new MySourceNonParallelism).setParallelism(1);
    val dataMap = dataStream.map(e => Tuple1(e));
    val partition = dataMap.partitionCustom(new MyPartition, 0)
      .map(e => {
        println("当前线程id: " + Thread.currentThread().getId + ",value=" + e._1);
        e._1
      });
    partition.print().setParallelism(1);

    env.execute("myParition");
  }

  @Test
  def countWindowsTest(): Unit = {
    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    wordDs.map((_, 1))
      .keyBy(0)
      // 累计单个key中3条就进行处理
      .countWindow(3)
      .sum(1)
      .print("测试：")

    env.execute()
  }

  /**
   * 滚动窗口 测试一
   */
  @Test
  def scrollWindow(): Unit = {
    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    wordDs.map(str => {
        val arr = str.split(",")
        (arr(0), arr(1).toLong, 1)
      }).keyBy(0)
      //      .timeWindow(Time.seconds(5)) // 过时的接口
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply((tuple: Tuple, window: TimeWindow, iterable: Iterable[(String, Long, Int)], out: Collector[String]) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        out.collect(s"window:[${window.getStart}-${window.getEnd}]: { ${iterable.mkString(",")}")
      })
      .print("windows:>>>")

    env.execute()
  }

  /**
   * 滚动窗口 测试二
   */
  @Test
  def scrollWindow2(): Unit = {
    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    wordDs.map(str => {
        val arr = str.split(",")
        (arr(0), arr(1).toLong, 1)
      }).keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(2)
      .print()

    env.execute()
  }

  /**
   * 滑动窗口 测试
   */
  @Test
  def slideWindow(): Unit = {
    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    wordDs.map(str => {
        val arr = str.split(",")
        (arr(0), arr(1).toLong, 1)
      })
      .keyBy(0)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(3)))
      //      .timeWindow(Time.seconds(5), Time.seconds(3))
      .apply((tuple: Tuple, window: TimeWindow, iterable: Iterable[(String, Long, Int)], out: Collector[String]) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        out.collect(s"window:[${window.getStart}-${window.getEnd}]: { ${iterable.mkString(",")}")
      })
      .print("windows:>>>")

    env.execute()
  }

  /**
   * 会话窗口 测试
   */
  @Test
  def sessionWindow(): Unit = {
    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    wordDs.map(str => {
        val arr = str.split(",")
        (arr(0), arr(1).toLong, 1)
      }).keyBy(0)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(15)))
      .apply((tuple: Tuple, window: TimeWindow, iterable: Iterable[(String, Long, Int)], out: Collector[String]) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        out.collect(s"window:[${window.getStart}-${window.getEnd}]: { ${iterable.mkString(",")}")
      })
      .print("windows:>>>")

    env.execute()
  }

  /**
   * 事件时间 --周期时间窗口
   * AssignerWithPeriodicWatermarks（周期性生成水位线）
   */
  @Test
  def eventTimePeriodicWindow(): Unit = {
    // 设置以事件时间为基准 1.12及以后的版本不在需要
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并行度
    env.setParallelism(1)
    // 设置10s生成一次水位线
    env.getConfig.setAutoWatermarkInterval(10000)

    val wordDs = env.socketTextStream("127.0.0.1", 3456)

    //    val tsDs = wordDs.map(str => {
    //      val strings = str.split(",")
    //      (strings(0), strings(1).toLong, 1)
    //    }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long, Int)]{
    //
    //      var maxTs :Long = 0;
    //      // 得到水位线，周期性调用这个方法，得到水位线，并设置延迟5秒
    //      override def getCurrentWatermark: Watermark = {
    //        println("waterTime: " + (maxTs - 5000));
    //        new Watermark(maxTs - 5000)
    //      }
    //
    //      // 负责抽取事件时间
    //      override def extractTimestamp(element: (String, Long, Int), previousElementTimestamp: Long): Long = {
    //        maxTs = maxTs.max(element._2 * 1000L)
    //        println("eventTime: " + element._2 * 1000L)
    //        element._2 * 1000L
    //      }
    //    })

    // 1.12及以后的新写法
    val tsDs = wordDs.map(str => {
      val strings = str.split(",")
      (strings(0), strings(1).toLong, 1)
    }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner(new TimestampAssignerSupplier[(String, Long, Int)] {

          override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[(String, Long, Int)] = {
            new TimestampAssigner[(String, Long, Int)] {
              override def extractTimestamp(t: (String, Long, Int), l: Long): Long = {
                t._2 * 1000L
              }
            }
          }
        })
    )

    val result = tsDs.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply((tuple: Tuple, window: TimeWindow, iterable: Iterable[(String, Long, Int)], out: Collector[String]) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        out.collect(s"window:[${window.getStart}-${window.getEnd}]: { ${iterable.mkString(",")}")
      })
    tsDs.print("water=>>")
    result.print("result=>>")

    env.execute()
  }

  /**
   * 事件时间，设置延迟接受时间 --周期时间窗口
   */
  @Test
  def eventTimePeriodicWindowLateness(): Unit = {
    env.setParallelism(1)
    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    val water = wordDs.map(str => {
      val strings = str.split(",")
      (strings(0), strings(1).toLong, 1)
    }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
      .withTimestampAssigner(new TimestampAssignerSupplier[(String, Long, Int)] {
        override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[(String, Long, Int)] = {
          new TimestampAssigner[(String, Long, Int)] {
            override def extractTimestamp(t: (String, Long, Int), l: Long): Long = {
              t._2 * 1000L
            }
          }
        }
      }))

    val result = water.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      // 具体延迟3秒 --延迟的水位线的值， 如窗口为10-15已经关闭，则 15<=水位线 <18期间的事件时间为10-15的事件都会被处理
      .allowedLateness(Time.seconds(3))
      .apply((tuple: Tuple, window: TimeWindow, iterable: Iterable[(String, Long, Int)], out: Collector[String]) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        out.collect(s"window:[${window.getStart}-${window.getEnd}]: { ${iterable.mkString(",")}")
      })

    water.print("water=>>")
    result.print("result=>>")
    env.execute()
  }

  /**
   * 事件时间，设置延迟接受时间,并设置侧输出流 --周期时间窗口
   */
  @Test
  def eventTimePeriodicWindowLatenessWithSideOut(): Unit = {
    env.setParallelism(1)
    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    val water = wordDs.map(str => {
      val strings = str.split(",")
      (strings(0), strings(1).toLong, 1)
    }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new TimestampAssignerSupplier[(String, Long, Int)] {
        override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[(String, Long, Int)] = {
          new TimestampAssigner[(String, Long, Int)] {
            override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2 * 1000L
          }
        }
      })
    )

    // 侧边输出流
    val outputTag = new OutputTag[(String, Long, Int)]("lateData")

    val result = water.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .allowedLateness(Time.seconds(3))
      .sideOutputLateData(outputTag)
      .apply((tuple: Tuple, window: TimeWindow, iterable: Iterable[(String, Long, Int)], out: Collector[String]) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        out.collect(s"window:[${window.getStart}-${window.getEnd}]: { ${iterable.mkString(",")}")
      })

    water.print("water>>>")
    result.print("window>>>")
    result.getSideOutput(outputTag).print("side>>>")

    env.execute()
  }

  /**
   * 标记水位线
   */
  @Test
  def markWatermark(): Unit = {
    env.setParallelism(1)
    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    val tsDs = wordDs.map(str => {
      val strings = str.split(",")
      (strings(0), strings(1).toLong, 1)
    }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier[(String, Long, Int)] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[(String, Long, Int)] = {
          new WatermarkGenerator[(String, Long, Int)] {
            private var currentMaxTimestamp: Long = _

            override def onEvent(t: (String, Long, Int), l: Long, watermarkOutput: WatermarkOutput): Unit = {
              // 更新当前最大时间戳
              currentMaxTimestamp = Math.max(currentMaxTimestamp, t._2 * 1000L)
              if (t._1.contains("later")) {
                println("间歇性生成了水位线")
                import org.apache.flink.api.common.eventtime.Watermark
                watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp))
              }
            }

            override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {

            }
          }
        }
      }).withTimestampAssigner(new TimestampAssignerSupplier[(String, Long, Int)] {
        override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[(String, Long, Int)] = {
          new TimestampAssigner[(String, Long, Int)] {
            override def extractTimestamp(t: (String, Long, Int), l: Long): Long = {
              t._2 * 1000L
            }
          }
        }
      })
    )

    val result = tsDs.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply((tuple: Tuple, window: TimeWindow, iterable: Iterable[(String, Long, Int)], out: Collector[String]) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        out.collect(s"window:[${window.getStart}-${window.getEnd}]: { ${iterable.mkString(",")}")
      })

    tsDs.print("water=>>")
    result.print("later=>>")

    env.execute()
  }

  /**
   * 多并行度生成水位线
   * -- 需要所有并行度中触发同一个窗口之后才会，才会触发一次
   */
  @Test
  def multiParallelismWatermark(): Unit = {
    env.setParallelism(5)

    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    val tsDs = wordDs.map(str => {
      val strings = str.split(",")
      (strings(0), strings(1).toLong, 1)
    }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner(new TimestampAssignerSupplier[(String, Long, Int)] {
          override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[(String, Long, Int)] = {
            new TimestampAssigner[(String, Long, Int)] {
              override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2 * 1000L
            }
          }
        })
    )
    val result = tsDs.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply((tuple: Tuple, window: TimeWindow, iterable: Iterable[(String, Long, Int)], out: Collector[String]) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        out.collect(s"window:[${sdf.format(new Date(window.getStart))}-${sdf.format(new Date(window.getEnd))}]:{ ${iterable.mkString(",")} }")
        //        out.collect(s"window:[${window.getStart}-${window.getEnd}]: { ${iterable.mkString(",")}")
      })

    tsDs.print("water=>>")
    result.print("calc=>>")

    env.execute()
  }

  /**
   * windows增量聚合
   */
  @Test
  def windowsIncrementalAggregation(): Unit = {
    env.setParallelism(1)
    val wordDs = env.socketTextStream("127.0.0.1", 3456);
    val tsDs = wordDs.map(str => {
      val strings = str.split(",")
      (strings(0), strings(1).toInt)
    })

    val result = tsDs.keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((a, b) => {
        (a._1, a._2 + b._2)
      })

    result.print("result=>>")

    env.execute()
  }

  /**
   * 自定义窗口
   */
  @Test
  def customeWindow(): Unit = {
    env.setParallelism(1)
    CustomWindowAssigner(1000L)
    val wordDs = env.socketTextStream("127.0.0.1", 3456)
    wordDs.map(str => {
      val strings = str.split(",")
      (strings(0), strings(1).toInt)
    }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner(new TimestampAssignerSupplier[(String, Int)] {
          override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[(String, Int)] = {
            new TimestampAssigner[(String, Int)] {
              override def extractTimestamp(t: (String, Int), l: Long): Long = System.currentTimeMillis()
            }
          }
        }))
      .keyBy(_._1)
      .window(CustomWindowAssigner())
      .sum(1)
      .print("result=>>")
    env.execute()
  }
}