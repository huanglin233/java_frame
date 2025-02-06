package com.hl.bigdata.flink.funciton;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huanglin
 * @date 2024/12/11 22:08
 */
public class ApplyWindowFunction implements WindowFunction<Tuple3<String, Integer, Integer>, String, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, Integer, Integer>> iterable, Collector<String> collector) throws Exception {
        List<String> windowStr = new ArrayList<>();
        iterable.forEach(tuple3 -> {
            windowStr.add("(" + tuple3.f0 + "," + tuple3.f1 + "," + tuple3.f2 + ")");
        });
        String print = "window[" + timeWindow.getStart() + ":" + timeWindow.getEnd() + "]:" + String.join(",", windowStr);
        collector.collect(print);
    }
}
