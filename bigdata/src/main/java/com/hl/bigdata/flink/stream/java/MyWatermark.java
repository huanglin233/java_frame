package com.hl.bigdata.flink.stream.java;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 自定义水位线策略
 * @author huanglin
 * @date 2024/07/04 18:00
 */
public class MyWatermark implements WatermarkStrategy<Tuple2<String, Long>> {

    @Override
    public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                return tuple2.f1;
            }
        };
    }

    @Override
    public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyPeriodicGenerator();
    }
}
