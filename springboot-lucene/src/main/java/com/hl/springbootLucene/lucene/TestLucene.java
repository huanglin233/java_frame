package com.hl.springbootLucene.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

@SuppressWarnings("deprecation")
public class TestLucene {

    public static void main(String[] args) throws IOException, ParseException, InvalidTokenOffsetsException {
        // 1.准备中文分词器
        IKAnalyzer analyzer = new IKAnalyzer();

        // 2.索引
        List<String> productNames = new ArrayList<>();
        productNames.add("飞利浦led灯泡e27螺口暖白球泡灯家用照明超亮节能灯泡转色温灯泡");
        productNames.add("飞利浦led灯泡e14螺口蜡烛灯泡3W尖泡拉尾节能灯泡暖黄光源Lamp");
        productNames.add("雷士照明 LED灯泡 e27大螺口节能灯3W球泡灯 Lamp led节能灯泡");
        productNames.add("飞利浦 led灯泡 e27螺口家用3w暖白球泡灯节能灯5W灯泡LED单灯7w");
        productNames.add("飞利浦led小球泡e14螺口4.5w透明款led节能灯泡照明光源lamp单灯");
        productNames.add("飞利浦蒲公英护眼台灯工作学习阅读节能灯具30508带光源");
        productNames.add("欧普照明led灯泡蜡烛节能灯泡e14螺口球泡灯超亮照明单灯光源");
        productNames.add("欧普照明led灯泡节能灯泡超亮光源e14e27螺旋螺口小球泡暖黄家用");
        productNames.add("聚欧普照明led灯泡节能灯泡e27螺口球泡家用led照明单灯超亮光源");
        Directory directory = createIndex(analyzer, productNames);

        // 3.查询器
        String keyWord = "护眼带光源";
        Query query = new QueryParser("name", analyzer).parse(keyWord);

        // 4.创建索引 reader
        DirectoryReader reader   = DirectoryReader.open(directory);
        // 基于 reader 创建搜索器
        IndexSearcher   searcher = new IndexSearcher(reader);
        // 指定每页要显示多少条数据
        int numberPerPage = 1000;
        System.out.printf("当前一共有%d条数据%n",productNames.size());
        System.out.printf("查询关键字是：\"%s\"%n",keyWord);
        // 执行搜索
        ScoreDoc[] scoreDocs = searcher.search(query, numberPerPage).scoreDocs;

        // 5.显示结果
        ShowSearchResults(searcher, scoreDocs, query, analyzer);
 
        // 关闭
        reader.close();
    }

    private static void ShowSearchResults(IndexSearcher searcher, ScoreDoc[] hits, Query query, IKAnalyzer ikAnalyzer) throws IOException, InvalidTokenOffsetsException {
        System.out.println("找到 " + hits.length + " 个命中.");
        System.out.println("序号\t匹配度得分\t结果");

        // 设置高亮参数
        SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter("<span style='color: red;'>", "</span>");
        Highlighter         highlighter         = new Highlighter(simpleHTMLFormatter, new QueryScorer(query));

        // 每一个ScoreDoc[] hits 就是一个搜索结果，首先把他遍历出来
        for(int i = 0; i < hits.length; i++) {
            ScoreDoc scoreDoc = hits[i];
            // 然后获取当前结果的docid, 这个docid相当于就是这个数据在索引中的主键
            int      docId    = scoreDoc.doc;
            // 再根据主键docid，通过搜索器从索引里把对应的Document取出来
            Document doc      = searcher.doc(docId);
            List<IndexableField> fields = doc.getFields();
            System.out.print((i + 1));
            System.out.print("\t" + scoreDoc.score);
            for(IndexableField field : fields) {
                TokenStream tokenStream  = ikAnalyzer.tokenStream(field.name(), new StringReader(doc.get(field.name())));
                String      bestFragment = highlighter.getBestFragment(tokenStream, doc.get(field.name()));
                System.out.println("\t" + bestFragment);
            }
            System.out.println();
        }
    }

    private static Directory createIndex(IKAnalyzer ikAnalyzer, List<String> products) throws IOException {
        // 创建内存索引
        Directory         directory = new RAMDirectory();
        // 根据中文分词器创建配置对象
        IndexWriterConfig config    = new IndexWriterConfig(ikAnalyzer);
        // 创建索引 writer
        IndexWriter       writer    = new IndexWriter(directory, config);

        // 遍历那10条数据，把他们挨个放进索引里
        for(String name : products) {
            addDoc(writer, name);
        }
        writer.close();

        return directory;
    }

    private static void addDoc(IndexWriter w, String name) throws IOException {
        // 每条数据创建一个Document，并把这个Document放进索引里
        Document document = new Document();
        document.add(new TextField("name", name, Field.Store.YES));
        w.addDocument(document);
    }
}