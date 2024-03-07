package com.hl.springbootELK.api;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;

public class TestElasticSearch4J {

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("192.168.56.101", 9200, "http")
            ));

    public static void main(String[] args) throws IOException {
        String indexName = "hl2";
        createIndex(indexName);
        if(!checkExistIndex(indexName)) {
            createIndex(indexName);
        }
        if(checkExistIndex(indexName)) {
            deleteIndex(indexName);
        }
        checkExistIndex(indexName);
        client.close();
    }

    private static boolean checkExistIndex(String indexName) throws IOException {
        boolean          result           = true;
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
        try {
            client.indices().open(openIndexRequest, RequestOptions.DEFAULT).isAcknowledged();
        } catch (ElasticsearchStatusException e) {
            String m = "Elasticsearch exception [type=index_not_found_exception, reason=no such index]";
            if (m.equals(e.getMessage())) {
                result = false;
            }
        }

        if(result) {
            System.out.println("索引: " + indexName + "是存在的");
        } else {
            System.out.println("索引: " + indexName + "不存在");
        }

        return result;
    }

    private static void deleteIndex(String indexName) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println("删除了索引: " + indexName);
    }

    private static void createIndex(String indexName) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println("创建了索引: " + indexName);
    }
}