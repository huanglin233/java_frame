package com.hl.springbootLucene.lucene;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.springframework.util.ResourceUtils;
import org.wltea.analyzer.lucene.IKAnalyzer;

@SuppressWarnings("deprecation")
public class TestLucene14w {

    public static void main(String[] args) throws Exception {
        // 1.准备分词器
        IKAnalyzer analyzer = new IKAnalyzer();
        // 2.创建索引
        Directory directory = createIndex(analyzer);
        /*
         * DeleteDocuments(Query query):根据Query条件来删除单个或多个Document
         * DeleteDocuments(Query[] queries):根据Query条件来删除单个或多个Document
         * DeleteDocuments(Term term):根据Term来删除单个或多个Document DeleteDocuments(Term[]
         * terms):根据Term来删除单个或多个Document DeleteAll():删除所有的Document
         */
        // 删除id = 51173的数据
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);
        writer.deleteDocuments(new Term("id", "51173"));
        writer.commit();
        writer.close();

        try (// 3.查询器
        Scanner scanner = new Scanner(System.in)) {
            while(true) {
                System.out.print("请输入查询关键字：");
                String keyWord = scanner.nextLine();
                System.out.println("当前关键字是："+keyWord);

                // 创建查询器
                Query query = new QueryParser("name", analyzer).parse(keyWord);
                // 4.搜索
                // 创建索引阅读器
                IndexReader   reader        = DirectoryReader.open(directory);
                IndexSearcher searcher      = new IndexSearcher(reader);
                int           numberPerPage = 10;
                ScoreDoc[]    scoreDocs     = searcher.search(query, numberPerPage).scoreDocs;
//                ScoreDoc[] scoreDocs = pageSearch1(query, searcher, 2, numberPerPage);
                // 5. 显示查询结果
                showSearchResults(searcher, scoreDocs, query, analyzer);

                // 6. 关闭查询
                reader.close();
            }
        }
    }

    // 把100条数据查出来，然后取最后10条。 优点是快，缺点是对内存消耗大。
    @SuppressWarnings("unused")
    private static ScoreDoc[] pageSearch1(Query query, IndexSearcher searcher, int pageNow, int pageSize) throws IOException {
        TopDocs    topDocs   = searcher.search(query, pageNow * pageSize);
        System.out.println("查询到的总条数\t"+topDocs.totalHits);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        List<ScoreDoc> sDocs = new ArrayList<>();
        int start = (pageNow -1) * pageSize;
        int end   = pageNow * pageSize;
        for(int i = start; i < end; i++) {
            sDocs.add(scoreDocs[i]);
        }
        ScoreDoc[] docs = sDocs.toArray(new ScoreDoc[] {});

        return docs;
    }

    // 是把第90条查询出来，然后基于这一条，通过searchAfter方法查询10条数据。 优点是内存消耗小，缺点是比第一种更慢
    @SuppressWarnings("unused")
    private static ScoreDoc[] pageSearch2(Query query, IndexSearcher searcher, int pageNow, int pageSize) throws IOException {
        int start = (pageNow -1) * pageSize;
        if(start == 0) {
            ScoreDoc[] scoreDocs = searcher.search(query, pageNow * pageSize).scoreDocs;

            return scoreDocs;
        }
        // 查询数据， 结束页面自前的数据都会查询到，但是只取本页的数
        TopDocs topDocs = searcher.search(query, start);
        // 获取到上一页最后一条
        ScoreDoc scoreDoc = topDocs.scoreDocs[start - 1];
        // 查询最后一条后的数据的一页数据
        TopDocs searchAfter = searcher.searchAfter(scoreDoc, query, pageSize);

        return searchAfter.scoreDocs;
    }

    private static void showSearchResults(IndexSearcher searcher, ScoreDoc[] hits, Query query, IKAnalyzer analyzer) throws Exception {
        System.out.println("找到 " + hits.length + " 个命中.");
 
        SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter("<span style='color:red'>", "</span>");
        Highlighter         highlighter         = new Highlighter(simpleHTMLFormatter, new QueryScorer(query));
 
        System.out.println("找到 " + hits.length + " 个命中.");
        System.out.println("序号\t匹配度得分\t结果");
        for (int i = 0; i < hits.length; ++i) {
            ScoreDoc scoreDoc = hits[i];
            int      docId    = scoreDoc.doc;
            Document d        = searcher.doc(docId);
            List<IndexableField> fields= d.getFields();
            System.out.print((i + 1) );
            System.out.print("\t" + scoreDoc.score);
            for (IndexableField f : fields) {
 
                if("name".equals(f.name())){
                    TokenStream tokenStream  = analyzer.tokenStream(f.name(), new StringReader(d.get(f.name())));
                    String      fieldContent = highlighter.getBestFragment(tokenStream, d.get(f.name()));
                    System.out.print("\t"+fieldContent);
                }
                else{
                    System.out.print("\t"+d.get(f.name()));
                }
            }
            System.out.println("<br>");
        }
    }

    private static Directory createIndex(IKAnalyzer analyzer) throws IOException {
        Directory         directory = new RAMDirectory();
        IndexWriterConfig config    = new IndexWriterConfig(analyzer);
        IndexWriter       writer    = new IndexWriter(directory, config);
        String           fileName   = "140k_products.txt";
        File             file       = ResourceUtils.getFile("classpath:" + fileName);
        List<Product>    products   = ProductUtil.file2List(file);
        int              total      = products.size();
        int              count      = 0;
        int              per        = 0;
        int              oldePer    = 0;
        for(Product product : products) {
            addDoc(writer, product);
            count++;
            per = count * 100 / total;
            if(per != oldePer) {
                oldePer = per;
                System.out.printf("索引中，总共要添加 %d 条记录，当前添加进度是： %d%% %n",total,per);
            }
        }
        writer.close();

        return directory;
    }

    private static void addDoc(IndexWriter writer, Product product) throws IOException {
        Document document = new Document();
        document.add(new TextField("id", String.valueOf(product.id), Field.Store.YES));
        document.add(new TextField("name", product.name, Field.Store.YES));
        document.add(new TextField("category", product.category, Field.Store.YES));
        document.add(new TextField("price", String.valueOf(product.price), Field.Store.YES));
        document.add(new TextField("place", product.place, Field.Store.YES));
        document.add(new TextField("code", product.code, Field.Store.YES));
        writer.addDocument(document);
    }
}