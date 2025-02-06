package com.hl.bigdata.flink.funciton;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;

/**
 * @author huanglin
 * @date 2024/12/11 22:30
 */
public class EventTimeAssignerFunction implements TimestampAssignerSupplier<Tuple3<String, Integer, Integer>>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public TimestampAssigner<Tuple3<String, Integer, Integer>> createTimestampAssigner(Context context) {
        return new TimestampAssigner<Tuple3<String, Integer, Integer>>() {
            @Override
            public long extractTimestamp(Tuple3<String, Integer, Integer> t, long l) {
                return t.f1 * 1000L;
            }
        };
    }
}
