package com.hl.bigdata.flink.stream.java.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author huanglin
 * @date 2024/03/08 18:20
 */
public class MySourceNonParallelism implements SourceFunction<Integer> {

    private int     count     = 1;
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

    }
}
