package com.hl.bigdata.flink.funciton;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;

/**
 * @author huanglin
 * @date 2024/12/16 21:51
 */
public class MyWatermarkGenerator implements WatermarkGeneratorSupplier<Tuple3<String, Integer, Integer>>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public WatermarkGenerator<Tuple3<String, Integer, Integer>> createWatermarkGenerator(Context context) {
        return new MyMarkWatermark();
    }
}
