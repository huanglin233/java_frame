package com.hl.springbootELK.api;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.lucene.analysis.sr.SerbianNormalizationFilter;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.util.ResourceUtils;

@SuppressWarnings("unused")
public class TestElasticSearch4J3 {

    private static RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9200, "http")
            ));
    private static String indexName = "how2java";

    public static void main(String[] args) throws Exception {
        // 确保索引存在
        if(!checkExistIndex(indexName)) {
            createIndex(indexName);
        }

        // 准备数据
        String fileName = "140k_products.txt";
        File file = ResourceUtils.getFile("classpath:" + fileName);
        List<Product> products = ProductUtil.file2List(file);
        System.out.println("准备数据，总计"+products.size()+"条");
//        batchInsert(products);

        getDocument(1);

        String     keyWord = "连衣裙";
        int        start   = 0;
        int        count   = 10;
        SearchHits search  = search(keyWord, start, count);
        SearchHit[] hits   = search.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
        client.close();
    }

    private static SearchHits search(String keyWord, int start, int count) throws Exception{
        SearchRequest       searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder builder       = new SearchSourceBuilder();
        // 关键字匹配
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("name", keyWord);
        // 模糊匹配
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        builder.query(matchQueryBuilder);
        // 第几页
        builder.from(start);
        // 第几条
        builder.size(count);
        searchRequest.source(builder);
        // 匹配度从高到低
        builder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();

        return hits;
    }

    @SuppressWarnings({ "rawtypes", "deprecation", "unchecked" })
    private static void batchInsert(List<Product> products) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();

        for(Product product : products) {
            Map map = product.toMap();
            IndexRequest indexRequest = new IndexRequest(indexName, "product", String.valueOf(product.getId())).source(map);
            bulkRequest.add(indexRequest);
        }
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println("批量插入完成");
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