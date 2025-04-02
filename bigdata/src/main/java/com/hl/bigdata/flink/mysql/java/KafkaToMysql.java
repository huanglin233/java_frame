package com.hl.bigdata.flink.mysql.java;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huanglin
 * @date 2025/04/01 22:34
 */
public class KafkaToMysql {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        KafkaSource<PageView> kafkaSource = KafkaSource.<PageView>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setTopics("to_mysql")
                .setGroupId("to_mysql")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new PageViewDeserializationSchema())
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .keyBy(PageView::getUserId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new WindowFunction<PageView, List<PageView>, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<PageView> input, Collector<List<PageView>> out) throws Exception {
                        List<PageView> pvs = new ArrayList<>();
                        input.forEach(pvs::add);
                        out.collect(pvs);
                    }
                }).addSink(new MysqlSink());

        env.execute("kafa to mysql");
    }
}
