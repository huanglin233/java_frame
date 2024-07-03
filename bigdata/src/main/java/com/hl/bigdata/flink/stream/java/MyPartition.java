package com.hl.bigdata.flink.stream.java;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区
 * @author huanglin
 * @date 2024/07/03 17:48
 */
public class MyPartition implements Partitioner<Integer> {

    @Override
    public int partition(Integer key, int i) {
        System.out.println("分区总数: " + i);

        if(key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
