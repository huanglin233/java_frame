package com.hl.bigdata.flink.operator.java;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.File;
import java.util.*;

/**
 * flink 批处理
 *
 * @author huanglin
 * @date 2024/12/26 22:06
 */
public class OperatorTest {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private DataStreamSource<String> fromElements;

    @Test
    public void fromCollection() throws Exception {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        DataStreamSource<Integer> dataStreamSource = env.fromCollection(data);
        dataStreamSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer o) throws Exception {
                return o + 1;
            }
        }).setParallelism(1).print();

        env.execute("fromCollection");
    }


    /*
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
     /

    /**
     * 广播变量
     */
    @Test
    public void broadcastTest() throws Exception {
        // 1.准备需要的广播数据
        List<Tuple2<String, Integer>> broadcastData = new ArrayList<>();
        broadcastData.add(new Tuple2<>("hl", 29));
        broadcastData.add(new Tuple2<>("ll", 30));
        broadcastData.add(new Tuple2<>("hh", 31));
        DataStreamSource<Tuple2<String, Integer>> broadcastDataSet = env.fromCollection(broadcastData);

        // 2.处理广播的数据
        MapStateDescriptor<String, Integer> key = new MapStateDescriptor<>("broadcast", Types.STRING, Types.INT);
        BroadcastStream<HashMap<String, Integer>> broadcast = broadcastDataSet.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> t2) throws Exception {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(t2.f0, t2.f1);
                return map;
            }
        }).broadcast(key);

        // 源数据
        SingleOutputStreamOperator<String> data = env.fromElements("hl", "ll", "hh")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                        Thread.sleep(2000); // 延迟2s等待广播数据初始完成
                        collector.collect(s);
                    }
                });
        data.keyBy(e -> e).connect(broadcast).process(new KeyedBroadcastProcessFunction<String, String, HashMap<String, Integer>, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
            }

            @Override
            public void processElement(String s, KeyedBroadcastProcessFunction<String, String, HashMap<String, Integer>, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                ReadOnlyBroadcastState<String, Integer> broadcastState = readOnlyContext.getBroadcastState(key);
                Integer age = broadcastState.get(s);
                collector.collect(s + ":" + age);
            }

            @Override
            public void processBroadcastElement(HashMap<String, Integer> mapState, KeyedBroadcastProcessFunction<String, String, HashMap<String, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                BroadcastState<String, Integer> broadcastState = context.getBroadcastState(key);
                broadcastState.putAll(mapState);
            }
        }).print();

        env.execute("broadcast");
    }

    /**
     * 累加器
     */
    @Test
    public void counterTest() throws Exception {
        DataStreamSource<String> data = env.fromElements("a", "b", "c", "b", "a");
        SingleOutputStreamOperator<String> result = data.map(new RichMapFunction<String, String>() {

            // 1.创建累加器
            private IntCounter counter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                // 2.注册累加器
                getRuntimeContext().addAccumulator("counter", counter);
            }

            @Override
            public String map(String arg0) throws Exception {
                // 3.使用累加器
                counter.add(1);
                return arg0;
            }


        }).setParallelism(8);

        // 4.获取累加器
        JobExecutionResult re = env.execute("counterTest");
        int num = re.getAccumulatorResult("counter");
        System.out.println(num);
    }

    /**
     * 连接两个流
     */
    @Test
    public void connectTest() throws Exception {
        DataStreamSource<String> data1 = env.fromElements("a", "b", "c");
        DataStreamSource<Integer> data2 = env.fromElements(1, 2, 3);
        data1.connect(data2).map(new CoMapFunction<String, Integer, String>() {

            @Override
            public String map1(String s) throws Exception {
                return s;
            }

            @Override
            public String map2(Integer integer) throws Exception {
                return integer.toString();
            }
        }).print();

        env.execute("cross");
    }

    /**
     * 分布式缓存
     */
    @Test
    public void registerCacheFile() throws Exception {
        // 注册一个文件
        env.registerCachedFile("hdfs://127.0.0.1:8020/hl/cache.txt", "cache.txt");

        DataStreamSource<String> dataStream = env.fromElements("a", "b", "c", "d", "e");
        dataStream.map(new RichMapFunction<String, String>() {
            Map<String, Integer> cache = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("cache.txt");
                FileUtils.readLines(file).forEach(line -> {
                    String[] split = line.split(",");
                    cache.put(split[0], Integer.parseInt(split[1]));
                });
            }

            @Override
            public String map(String s) {
                Integer i = cache.get(s);

                return i != null ? s + ":" + i : s + ":null";
            }
        }).print();

        env.execute("registerCacheFile");
    }

    /**
     * 去重
     */
    @Test
    public void distinct() throws Exception {
        // 1.准备数据
        ArrayList<String> list = new ArrayList<>();
        list.add("ccc bbb");
        list.add("how are you");
        list.add("you are good");

        // 2.读取数据
        DataStreamSource<String> data = env.fromCollection(list);

        // 3.处理数据
        data.flatMap(new FlatMapFunction<String, String>() {

                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] worlds = s.split("\\W+");
                        for (String world : worlds) {
                            collector.collect(world);
                        }
                    }
                }).keyBy(word -> word)
                .process(new KeyedProcessFunction<String, String, String>() {
                    private ValueState<Boolean> seen;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen", Types.BOOLEAN));
                    }

                    @Override
                    public void processElement(String s, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                        if (seen.value() == null) {
                            seen.update(true);
                            collector.collect(s);
                        }
                    }
                }).print();


        env.execute("distinct");
    }

}