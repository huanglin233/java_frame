package com.hl.bigdata.flink.es.java;

import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author huanglin
 * @date 2025/03/15 19:47
 */
public class ElasticSearchSinkUtil {

    public static <T> void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                                   SingleOutputStreamOperator<T> data, ElasticsearchEmitter<T> sendEmitter) {

        Elasticsearch7SinkBuilder<T> builder = new Elasticsearch7SinkBuilder<>();
        builder.setHosts(hosts.toArray(new HttpHost[0]))
                .setBulkFlushMaxActions(bulkFlushMaxActions)
                .setEmitter(sendEmitter);

        data.sinkTo(builder.build()).setParallelism(parallelism);
    }

    public static IndexRequest createIndexRequest(String index, String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index(index)
                .id(element)
                .source(json);
    }
}
