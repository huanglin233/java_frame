package com.hl.bigdata.flink.sql.java.udf;

import com.hl.bigdata.flink.sql.vo.AgeAvgAccum;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * udf 聚合函数
 *  -- 把一行或多行数据聚合为1个值
 * @author huanglin
 * @date 2024/11/13 15:39
 */
public class UDAGGFunction extends AggregateFunction<Integer, AgeAvgAccum> {

    @Override
    public Integer getValue(AgeAvgAccum ageAvgAccum) {
        if(ageAvgAccum.count == 0) {
            return 0;
        } else {
            return (int) (ageAvgAccum.sum / ageAvgAccum.count);
        }
    }

    @Override
    public AgeAvgAccum createAccumulator() {
        return new AgeAvgAccum();
    }

    /**
     * 累加器 -- 每次更新数据时，更新聚合状态
     */
    public void accumulate(AgeAvgAccum  acc, long price, int num) {
        acc.sum += price * num;
        acc.count += 1;
    }

    /**
     * 在撤销某条数据的影响时，更新当前的聚合状态
     */
    public void retract(AgeAvgAccum acc, long price, int num) {
        acc.sum -= price * num;
        acc.count -= 1;
    }

    /**
     * 合并两个聚合态
     */
    public void merge(AgeAvgAccum acc, Iterable<AgeAvgAccum> its) {
        for (AgeAvgAccum acc1 : its) {
            acc.sum += acc1.sum;
            acc.count += acc1.count;
        }
    }

    public void resetAccumulator(AgeAvgAccum acc) {
        acc.count = 0;
        acc.sum = 0;
    }
}
