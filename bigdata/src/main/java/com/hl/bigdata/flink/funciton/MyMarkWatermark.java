package com.hl.bigdata.flink.funciton;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;

/**
 * 自定义水位线策略
 *
 * @author huanglin
 * @date 2024/12/16 21:43
 */
public class MyMarkWatermark implements WatermarkGenerator<Tuple3<String, Integer, Integer>>, Serializable {

    private static final long serialVersionUID = 1L;

    private long maxTimestamp = 0L;

    @Override
    public void onEvent(Tuple3<String, Integer, Integer> t, long l, WatermarkOutput watermarkOutput) {
        // 更新当前最大时间戳
        maxTimestamp = Math.max(maxTimestamp, t.f1 * 1000L);
        if (t.f0.contains("later")) {
            System.out.println("mark==>" + maxTimestamp);
            watermarkOutput.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(maxTimestamp));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

    }
}
