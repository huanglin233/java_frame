package com.hl.bigdata.flink.stream.java.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author huanglin
 * @date 2024/03/12 17:20
 */
public class MySourceParallelism extends RichParallelSourceFunction<Integer> {

    private Integer count = 1;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while(isRunning) {
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open multiple source ......");
        super.open(parameters);
    }
}
