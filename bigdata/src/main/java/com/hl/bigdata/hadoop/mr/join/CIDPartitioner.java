package com.hl.bigdata.hadoop.mr.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区
 * @author huanglin
 * @date 2021/08/08 15/43/53
 */
public class CIDPartitioner extends Partitioner<ComboKey, Text>{

    @Override
    public int getPartition(ComboKey key, Text value, int numPartitions) {
         return key.getCustomerId() % numPartitions;
    }
}