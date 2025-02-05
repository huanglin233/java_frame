package com.hl.bigdata.flink.sql.java.udf;

import com.hl.bigdata.flink.sql.vo.Top;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;


/**
 * 表值聚合函数 udf
 * -- 把一行或多行数据聚合为多行
 * @author huanglin
 * @date 2024/11/13 22:28
 */
public class UDTAGGFunction extends TableAggregateFunction<Tuple2<Integer, Integer>, Top> {

    @Override
    public Top createAccumulator() {
        Top top = new Top();
        top.first  = Integer.MIN_VALUE;
        top.second = Integer.MIN_VALUE;

        return top;
    }

    public void accumulate(Top acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first  = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

    public void merge(Top acc, Iterable<Top> its) {
        for (Top acc1 : its) {
            accumulate(acc, acc1.first);
            accumulate(acc, acc1.second);
        }
    }

    public void emitValue(Top acc, Collector<Tuple2<Integer, Integer>> out) {
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MAX_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }
}
