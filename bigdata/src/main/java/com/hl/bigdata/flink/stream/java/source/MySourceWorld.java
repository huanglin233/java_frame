package com.hl.bigdata.flink.stream.java.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author huanglin
 * @date 2024/07/03 11:49
 */
public class MySourceWorld extends RichSourceFunction<String> {

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while(true) {
            String world = "aa bb cc dd ";
            sourceContext.collect(world);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
