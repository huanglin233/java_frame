package com.hl.springbootELK.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.lucene.index.CheckIndex.Options;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;

@SuppressWarnings("unused")
public class TestElasticSearch4J2 {

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("192.168.56.101", 9200, "http")
            ));
    private static String indexName = "hl2";

    public static void main(String[] args) throws IOException {
        // 确保索引存在
        if(!checkExistIndex(indexName)) {
            createIndex(indexName);
        }

        // 准备数据
        Product product = new Product();
        product.setId(1);
        product.setName("product 1");

        // 增加文档
        addDocument(product);

        // 获取文档
        addDocument(product);

        // 修改数据
        product.setName("product 2");
        // 修改文档
        updateDocument(product);
        // 获取文档
        getDocument(1);

        // 删除文档
        deleteDocument(1);
        // 获取文档
        getDocument(1);

        client.close();
    }

    @SuppressWarnings("deprecation")
    private static void deleteDocument(int id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, "product", String.valueOf(id));
        client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println("已经从ElasticSearch服务器上删除id="+id+"的文档");
    }

    @SuppressWarnings("deprecation")
    private static void updateDocument(Product product) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(indexName, "product", String.valueOf(product.getId())).doc("name", product.getName());
        client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println("已经在ElasticSearch服务器修改产品为："+product);
    }

    @SuppressWarnings("deprecation")
    private static void getDocument(int id) throws IOException {
        GetRequest  getRequest  = new GetRequest(indexName, "product", String.valueOf(id));
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        if(!getResponse.isExists()) {
            System.out.println("检查到服务器上 "+"id="+id+ "的文档不存在");
        } else {
            String source = getResponse.getSourceAsString();
            System.out.print("获取到服务器上 "+"id="+id+ "的文档内容是：");
            System.out.println(source);
        }
    }

    @SuppressWarnings("deprecation")
    private static void addDocument(Product product) throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", product.getName());
        IndexRequest indexRequest = new IndexRequest(indexName, "product", String.valueOf(product.getId())).source(jsonMap);
        client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("已经向ElasticSearch服务器增加产品："+product);
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
