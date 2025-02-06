package com.hl.bigdata.flink.funciton;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author huanglin
 * @date 2024/12/11 21:58
 */
public class StrSplitMapFunction implements MapFunction<String, Tuple3<String, Integer, Integer>> {
    @Override
    public Tuple3<String, Integer, Integer> map(String s) throws Exception {
        String[] split = s.split(",");
        return Tuple3.of(split[0], Integer.valueOf(split[1]), 1);
    }
}
