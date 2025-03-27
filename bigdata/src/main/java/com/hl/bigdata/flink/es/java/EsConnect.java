package com.hl.bigdata.flink.es.java;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author huanglin
 * @date 2025/03/15 19:42
 */
public class EsConnect {

    static Configuration conf = new Configuration();
    static StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

    public static void main(String[] args) throws Exception {

        DataStreamSource<String> source = env.fromElements("data1", "data2", "data3");
        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("127.0.0.1", 9200));
        String index = "test";
        ElasticSearchSinkUtil.addSink(hosts, 100, 1, source,
                (element, context, indexer) -> indexer.add(ElasticSearchSinkUtil.createIndexRequest(index, element)));

        env.execute("exec es sink");
    }


}
