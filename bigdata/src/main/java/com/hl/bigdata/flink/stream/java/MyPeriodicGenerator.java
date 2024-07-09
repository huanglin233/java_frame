package com.hl.bigdata.flink.stream.java;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 自定义水位线生成策略
 *
 * @author huanglin
 * @date 2024/07/04 18:06
 */
public class MyPeriodicGenerator implements WatermarkGenerator<Tuple2<String, Long>> {

    Long  currentMaxTimestamp    = 0L;
    final Long maxOutOfOrderness = 1000L; // 最大允许乱序时间10s

    @Override
    public void onEvent(Tuple2<String, Long> stringLongTuple2, long l, WatermarkOutput watermarkOutput) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, stringLongTuple2.f1);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        // 发射水位线，默认 200ms 调用一次
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness));
    }
}
