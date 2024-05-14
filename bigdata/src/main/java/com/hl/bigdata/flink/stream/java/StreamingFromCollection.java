package com.hl.bigdata.flink.stream.java;

import com.hl.bigdata.flink.stream.java.source.MySourceNonParallelism;
import com.hl.bigdata.flink.stream.java.source.MySourceParallelism;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author huanglin
 * @date 2024/03/08 17:27
 */
public class StreamingFromCollection {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        SideOutputDataStream<String> evenStream = processStream.getSideOutput(even);
        SideOutputDataStream<String> oddStream = processStream.getSideOutput(odd);


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
    public void checkPoint() {
        //
    }
}
