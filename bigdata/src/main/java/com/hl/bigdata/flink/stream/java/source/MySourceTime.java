package com.hl.bigdata.flink.stream.java.source;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author huanglin
 * @date 2024/07/04 15:37
 */
public class MySourceTime extends RichSourceFunction<String>{
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while(true) {
            String str = "hello" + RandomUtils.nextInt(1, 5);
            sourceContext.collect(str + "," + (System.currentTimeMillis() - RandomUtils.nextLong(100, 1000)));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}