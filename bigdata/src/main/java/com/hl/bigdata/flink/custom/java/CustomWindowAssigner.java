package com.hl.bigdata.flink.custom.java;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author huanglin
 * @date 2025/02/11 22:45
 */
public class CustomWindowAssigner<Long> extends WindowAssigner<Object, TimeWindow> {

    private final long windowSize;

    public CustomWindowAssigner(long windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext windowAssignerContext) {
        System.out.println(timestamp);
        List<TimeWindow> windows = new ArrayList<>();
        // 自定义窗口逻辑，这里假设窗口从某个时间点开始
        long start = timestamp - (timestamp % windowSize);
        long end = start + windowSize;
        System.out.println("window start: " + start + ", end: " + end);
        windows.add(new TimeWindow(start, end));

        return windows;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        // 使用默认的处理时间触发器
        return ProcessingTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false; // 如果事件事件返回 true
    }
}
