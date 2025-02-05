package com.hl.bigdata.flink.stream.java;

import com.hl.bigdata.flink.funciton.*;
import com.hl.bigdata.flink.stream.java.source.MySourceNonParallelism;
import com.hl.bigdata.flink.stream.java.source.MySourceParallelism;
import com.hl.bigdata.flink.stream.java.source.MySourceTime;
import com.hl.bigdata.flink.stream.java.source.MySourceWorld;
import com.hl.bigdata.flink.stream.vo.WordWithCount;
import com.sun.istack.Nullable;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * @author huanglin
 * @date 2024/03/08 17:27
 */
public class StreamingFromCollection {

    static Configuration conf = new Configuration();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

    @Test
    public void streamDemo() throws Exception {
        List<Integer> data = Arrays.asList(10, 15, 20);

        DataStreamSource<Integer> collection = env.fromCollection(data);
        SingleOutputStreamOperator<Integer> map = collection.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer val) throws Exception {
                return val + 1;
            }
        });
        map.print();

        env.execute();
    }

    @Test
    public void streamNonParallelismResource() throws Exception {
        DataStreamSource<Integer> dss = env.addSource(new MySourceNonParallelism()).setParallelism(1);
        SingleOutputStreamOperator<Integer> soo = dss.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer val) throws Exception {
                System.out.println("接收到数据: " + val);
                return val;
            }
        });

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SingleOutputStreamOperator<Integer> sum = soo.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print();

        env.execute();
    }

    @Test
    public void streamMultipleParallelism() throws Exception {
        DataStreamSource<Integer> dataStream = env.addSource(new MySourceParallelism()).setParallelism(3);
        SingleOutputStreamOperator<Integer> mapStream = dataStream.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer val) throws Exception {
                System.out.println("接收到数据: " + val);
                return val;
            }
        });

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SingleOutputStreamOperator<Integer> sum = mapStream.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print();

        env.execute();
    }

    @Test
    public void filter() throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<Integer> dataStream = env.addSource(new MySourceNonParallelism()).setParallelism(1);
        dataStream.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer val) throws Exception {
                System.out.println("接收到数据: " + val);

                return val;
            }
        }).filter(val -> val % 2 == 0).map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer val) throws Exception {
                System.out.println("过滤后数据: " + val);

                return val;
            }
        }).timeWindowAll(Time.seconds(2)).sum(0).print();

        env.execute("filter");
    }

    /**
     * 根据规则把一个数据流切分为多个数据流
     *
     * @throws Exception
     */
    @Test
    public void split() throws Exception {
        //1.使用本地模式
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<Integer> dataStream = env2.addSource(new MySourceNonParallelism()).setParallelism(1);

        env2.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        dataStream.timeWindowAll(Time.seconds(2)).sum(0).print();

        OutputTag<String> even = new OutputTag<String>("even") {
        };
        OutputTag<String> odd = new OutputTag<String>("odd") {
        };

        SingleOutputStreamOperator<String> processStream = dataStream.process(new ProcessFunction<Integer, String>() {
            @Override
            public void processElement(Integer val, ProcessFunction<Integer, String>.Context context, Collector<String> collector) throws Exception {
                if (val % 2 == 0) {
                    context.output(even, val + "->even");
                } else {
                    context.output(odd, val + "odd");
                }
            }
        });
        DataStream<String> evenStream = processStream.getSideOutput(even);
        DataStream<String> oddStream = processStream.getSideOutput(odd);

        evenStream.print();
        oddStream.print();

        env2.execute("split");
    }

    /**
     * 合并两个数据流类型相同的流
     *
     * @throws Exception
     */
    @Test
    public void union() throws Exception {
        DataStreamSource<Integer> dataStream1 = env.addSource(new MySourceNonParallelism()).setParallelism(1);
        DataStreamSource<Integer> dataStream2 = env.addSource(new MySourceNonParallelism()).setParallelism(1);

        DataStream<Integer> unionStream = dataStream1.union(dataStream2);
        SingleOutputStreamOperator<Integer> mapStream = unionStream.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer val) throws Exception {
                System.out.println("接受到的数据: " + val);

                return val;
            }
        });

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        mapStream.timeWindowAll(Time.seconds(2)).sum(0).print();

        env.execute("union");
    }

    /**
     * 合并两种类型不一样的数据流
     *
     * @throws Exception
     */
    @Test
    public void connect() throws Exception {
        DataStreamSource<Integer> dataStream1 = env.addSource(new MySourceNonParallelism()).setParallelism(1);
        DataStreamSource<Integer> dataStream2 = env.addSource(new MySourceNonParallelism()).setParallelism(1);

        SingleOutputStreamOperator<String> mapStream = dataStream1.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer val) throws Exception {
                return "str" + val;
            }
        });
        ConnectedStreams<String, Integer> connectStream = mapStream.connect(dataStream2);
        SingleOutputStreamOperator<Object> result = connectStream.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String val) throws Exception {
                return val;
            }

            @Override
            public Object map2(Integer val) throws Exception {
                return val;
            }
        });
        result.print().setParallelism(1);

        env.execute("connect");
    }

    @Test
    public void broadcast() throws Exception {
        env.setParallelism(4);
        DataStreamSource<Integer> dataStream = env.addSource(new MySourceNonParallelism()).setParallelism(1);
        SingleOutputStreamOperator<Integer> mapStream = dataStream.broadcast().map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer val) throws Exception {
                long id = Thread.currentThread().getId();
                System.out.println("线程id: " + id + ", 接收到数据: " + val);

                return val;
            }
        });
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        mapStream.timeWindowAll(Time.seconds(2)).sum(0).print().setParallelism(1);

        env.execute("broadcast");
    }

    @Test
    public void checkPoint() throws Exception{
        // 每隔1000 ms进行启动一个检查点 --设置checkpoint得周期
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点之间得间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置检查点执行的时间,时间内没有执行完则丢弃
        env.getCheckpointConfig().setCheckpointTimeout(6000);
        // 同一时间只允许一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // flink处理程序cancel后,会保留checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // flink处理程序被cancel后,会删除checkpoint数据,只有执行失败得时候才会保存checkpoint数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // 设置checkpoint存储方式 --可flink客户端flink-conf.yaml配置文件中进行全局配置
//        env.setStateBackend(new MemoryStateBackend());
//        env.setStateBackend(new FsStateBackend("/hdfs/...."));
        DataStreamSource<String>  dateStream = env.addSource(new MySourceWorld());
        DataStream<WordWithCount> sum        = dateStream.flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                        String[] strs = s.split("\\s");
                        for (String word : strs) {
                            collector.collect(new WordWithCount(word, 1));
                        }
                    }
                }).keyBy(e -> e.word)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(1))) // 指定时间窗口为2秒,指定时间间隔为1秒
//                        .sum("count");
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount wordWithCount, WordWithCount t1) throws Exception {
                        return new WordWithCount(wordWithCount.word, wordWithCount.count + t1.count);
                    }
                });
        sum.print().setParallelism(1);
        env.execute("source window count");
    }

    @Test
    public void myParition() throws Exception {
        env.setParallelism(2);

        DataStreamSource<Integer> dataStream = env.addSource(new MySourceNonParallelism());
        // 把long数据转换成tuple类型
        SingleOutputStreamOperator<Integer> map = dataStream.map(new MapFunction<Integer, Tuple1<Integer>>() {
                    @Override
                    public Tuple1<Integer> map(Integer integer) throws Exception {
                        return new Tuple1<Integer>(integer);
                    }
                }).partitionCustom(new MyPartition(), 0)
                .map(new MapFunction<Tuple1<Integer>, Integer>() {
                    @Override
                    public Integer map(Tuple1<Integer> integerTuple1) throws Exception {
                        System.out.println("当前线程id: " + Thread.currentThread().getId() + ",value: " + integerTuple1);

                        return integerTuple1.getField(0);
                    }
                });
        map.print().setParallelism(1);

        env.execute("myParition");
    }

    @Test
    public void streamingWindowWatermark() throws Exception {
        // 设置使用eventtime,默认是使用processingtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> dataStream = env.addSource(new MySourceTime());
        SingleOutputStreamOperator<Tuple2<String, Long>> map = dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0], Long.parseLong(split[1]));
            }
        });

        // 内置有序流的watermark生成器
//        map.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
//                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
//
//                    /**
//                     * 获取时间戳
//                     * @param tuple2
//                     * @param l
//                     * @return
//                     */
//                    @Override
//                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
//                        return tuple2.f1;
//                    }
//        }));
        // 内置乱序流的watermark生成器
//        map.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
//
//                    @Override
//                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
//                        return tuple2.f1;
//                    }
//                }));

//        SingleOutputStreamOperator<Tuple2<String, Long>> watermark = map.assignTimestampsAndWatermarks(new MyWatermark());

        SingleOutputStreamOperator<Tuple2<String, Long>> watermark = map.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            Long  currentMaxTimestamp    = 0L;
            final Long maxOutOfOrderness = 1000L; // 最大允许乱序时间10s

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            /**
             * 定义水位线watermark的生成
             * 默认100ms调用一次
             * @return
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            /**
             * 解析提取
             * @param tuple2
             * @param l
             * @return
             */
            @Override
            public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                Long f1 = tuple2.f1;
                currentMaxTimestamp = Math.max(f1, currentMaxTimestamp);
                Long id = Thread.currentThread().getId();
                System.out.println("线程id: " + id + ",key: " + tuple2.f0 + ",eventime: [" + tuple2.f1 + "|" + sdf.format(tuple2.f1) + "], currentMaxTimestamp: [" +
                        currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "],watermark: [" + getCurrentWatermark().getTimestamp() + "|"
                        + sdf.format(getCurrentWatermark().getTimestamp()) + "]");

                return f1;
            }
        });

        SingleOutputStreamOperator<String> window = watermark.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3))) // 按照消息event分窗口
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {

                    /**
                     * 对窗口内的数据进行排序
                     * @param tuple
                     * @param timeWindow
                     * @param iterable
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        String     key  = tuple.toString();
                        List<Long> list = new ArrayList<>();
                        Iterator<Tuple2<String, Long>> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            Tuple2<String, Long> next = iterator.next();
                            list.add(next.f1);
                        }
                        Collections.sort(list);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        if(!list.isEmpty()) {
                            String ret = key + "," + list.size() + ","
                                    + sdf.format(list.get(0)) + ","
                                    + sdf.format(list.get(list.size() - 1))
                                    + sdf.format(timeWindow.getStart())
                                    + ',' + sdf.format(timeWindow.getEnd());

                            collector.collect(ret);
                        }
                    }
                });
        window.print();

        env.execute("eventtime-watermark");
    }

    @Test
    public void streamingWindowWatermark2() throws Exception {
        DataStreamSource<String> dataStream = env.addSource(new MySourceTime());
        SingleOutputStreamOperator<Tuple2<String, Long>> map = dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] split = s.split(",");

                return new Tuple2<>(split[0], Long.valueOf(split[1]));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> watermark = map.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                        return tuple2.f1;
                    }
                }));
        OutputTag<Tuple2<String, Long>>    outputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};
        SingleOutputStreamOperator<String> window    = watermark.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(5)) // 允许迟到5s
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        String key = tuple.toString();
                        List<Long> list = new ArrayList<>();
                        Iterator<Tuple2<String, Long>> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            Tuple2<String, Long> next = iterator.next();
                            list.add(next.f1);
                        }
                        Collections.sort(list);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        if (!list.isEmpty()) {
                            String ret = key + "," + list.size() + ","
                                    + sdf.format(list.get(0)) + ","
                                    + sdf.format(list.get(list.size() - 1))
                                    + sdf.format(timeWindow.getStart())
                                    + ',' + sdf.format(timeWindow.getEnd());

                            collector.collect(ret);
                        }
                    }
                });
        DataStream<Tuple2<String, Long>> sideOutput = window.getSideOutput(outputTag);
        sideOutput.print();
        window.print();

        env.execute("watermark");
    }

    @Test
    public void countWindowsTest() throws Exception {
        DataStreamSource<String> wordDs = env.socketTextStream("127.0.0.1", 3456);
        wordDs.map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        return new Tuple2<>(s, 1L);
                    }
                })
                .keyBy(0)
                // 累计单个key中处理3条就进行处理
                .countWindow(3)
                .sum(1)
                .print("窗口测试=>>");

        env.execute();
    }

    /**
     * 滚动窗口 测试一
     */
    @Test
    public void scrollWindow() throws Exception {
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 3456);
        dataStreamSource.map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(String s) throws Exception {
                return new Tuple3<>(s, s.length(), 1);
            }
        }).keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple3<String, Integer, Integer>, String, Tuple, TimeWindow>() {

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, Integer, Integer>> iterable, Collector<String> collector) throws Exception {
                        List<String> windowStr = new ArrayList<>();
                        iterable.forEach(tuple3 -> {
                            windowStr.add("(" + tuple3.f0 + "," + tuple3.f1 + "," + tuple3.f2 + ")");
                        });
                        String print = "window[" + timeWindow.getStart() + ":" + timeWindow.getEnd() + "]:" + String.join(",", windowStr);
                        collector.collect(print);
                    }
                })
                .print("windows:>>>");
        env.execute();
    }

    /**
     * 滚动窗口 测试二
     * @throws Exception
     */
    @Test
    public void scrollWindow2() throws Exception {
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 3456);
        dataStreamSource.map(new StrSplitMapFunction())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(2)
                .print("sum=>>");

        env.execute();
    }

    /**
     * 滑动窗口 测试
     */
    @Test
    public void slideWindow() throws Exception {
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 3456);
        dataStreamSource.map(new StrSplitMapFunction())
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(3)))
                .apply(new ApplyWindowFunction())
                .print("window:>>>");

        env.execute();
    }

    /**
     * 会话窗口 测试
     */
    @Test
    public void sessionWindow() throws Exception {
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 3456);
        dataStreamSource.map(new StrSplitMapFunction())
                .keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(15)))
                .apply(new ApplyWindowFunction())
                .print("window:>>>");

        env.execute();
    }

    /**
     * 事件时间 --周期时间窗口
     * AssignerWithPeriodicWatermarks(周期性水位线)
     */
    @Test
    public void eventTimePeriodicWindow() throws Exception {
        env.setParallelism(1);
        // 设置10s生成一次水位线
        env.getConfig().setAutoWatermarkInterval(10000);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 3456);
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> tsDs = dataStreamSource.map(new StrSplitMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new EventTimeAssignerFunction())
                );

        SingleOutputStreamOperator<String> result = tsDs.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new ApplyWindowFunction());

        tsDs.print("water==>");
        result.print("result==>");

        env.execute();
    }

    /**
     * 事件时间，设置延迟接受时间 --周期时间窗口
     */
    @Test
    public void eventTimePeriodicWindowLateness() throws Exception {
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 3456);
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> water = dataStreamSource.map(new StrSplitMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new EventTimeAssignerFunction())
                );
        SingleOutputStreamOperator<String> result = water.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                .apply(new ApplyWindowFunction());

        water.print("water=>>");
        result.print("result=>>");

        env.execute();
    }

    /**
     * 事件时间，设置延迟接受时间，并设置侧输出流 --周期时间窗口
     */
    @Test
    public void eventTimePeriodicWindowLatenessWithSideOut() throws Exception {
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 3456);
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> water = dataStreamSource.map(new StrSplitMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new EventTimeAssignerFunction())
                );

        // 侧边输出流
        OutputTag<Tuple3<String, Integer, Integer>> outputTag = new OutputTag<Tuple3<String, Integer, Integer>>("lateData"){};

        SingleOutputStreamOperator<String> result = water.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(outputTag)
                .apply(new ApplyWindowFunction());

        water.print("water=>>");
        result.print("window=>>");
        result.getSideOutput(outputTag).print("side=>>");

        env.execute();
    }

    /**
     * 标记数位线
     */
    @Test
    public void markWatermark() throws Exception {
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 3456);
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> tsDs = dataStreamSource.map(new StrSplitMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forGenerator(
                                new MyWatermarkGenerator()
                        ).withTimestampAssigner(new EventTimeAssignerFunction())
                );

        SingleOutputStreamOperator<String> result = tsDs.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new ApplyWindowFunction());

        tsDs.print("water==>");
        result.print("later==>");

        env.execute();
    }

    /**
     * 窗口聚合函数
     */
    @Test
    public void windowAggregateFunction() throws Exception {
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 3456);
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataMap = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0], Integer.parseInt(split[1]));
            }
        });

        SingleOutputStreamOperator<Integer> result = dataMap.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Tuple2<String, Integer> t, Integer o) {
                        return o + t.f1;
                    }

                    @Override
                    public Integer getResult(Integer o) {
                        return o;
                    }

                    @Override
                    public Integer merge(Integer o, Integer acc1) {
                        return o + acc1;
                    }
                });

        result.print("result=>>");

        env.execute();
    }
}
