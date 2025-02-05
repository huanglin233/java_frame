package com.hl.bigdata.flink.sql.java.udf;

import com.hl.bigdata.flink.sql.vo.Top2;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * 表值聚合函数 udf --使用emitUpdateWithRetract
 * -- 把一行或多行数据聚合为多行
 *
 * @author huanglin
 * @date 2024/11/14 21:25
 */
public class UDTAGGFunction2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2> {

    @Override
    public Top2 createAccumulator() {
        Top2 top2 = new Top2();

        top2.first     = Integer.MIN_VALUE;
        top2.second    = Integer.MIN_VALUE;
        top2.oldFirst  = Integer.MIN_VALUE;
        top2.oldSecond = Integer.MIN_VALUE;

        return top2;
    }

    public void accumulate(Top2 acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first  = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

    public void emitValue(Top2 acc, Collector<Tuple2<Integer, Integer>> out) {
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MAX_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }

    public void emitUpdateWithRetract(Top2 acc, RetractableCollector<Tuple2<Integer, Integer>> out) {
        if (!acc.first.equals(acc.oldFirst)) {
            if (acc.oldFirst != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldFirst, 1));
            }
            out.collect(Tuple2.of(acc.first, 1));
            acc.oldFirst = acc.first;
        }

        if (!acc.second.equals(acc.oldSecond)) {
            if (acc.oldSecond != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldSecond, 2));
            }
            out.collect(Tuple2.of(acc.second, 2));
            acc.oldSecond = acc.second;
        }
    }
}
