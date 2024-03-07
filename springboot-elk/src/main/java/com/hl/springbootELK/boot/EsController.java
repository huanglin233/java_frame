package com.hl.springbootELK.boot;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery.ScoreMode;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EsController {

    @Autowired
    CategoryDao categoryDao;

    @SuppressWarnings("deprecation")
    @GetMapping("/listCategory")
    public String listCategory() {
        String query = "商品";
        NativeSearchQuery entitySearchQuery = getEntitySearchQuery(1, 10, query);
        Page<Category> search = categoryDao.search(entitySearchQuery);
       for(Category category : search.getContent()) {
           System.out.println(category.toString());
       }

        return search.toString();
    }

    private NativeSearchQuery getEntitySearchQuery(int start, int size, String searchContent) {
        //构建查询条件
        FunctionScoreQueryBuilder query = QueryBuilders.functionScoreQuery(QueryBuilders.matchQuery("name", searchContent), ScoreFunctionBuilders.weightFactorFunction(100));
        // 设置权重分 求和模式
        query.scoreMode(ScoreMode.SUM);
        // 设置权重分最低分
        query.setMinScore(10);
        //高亮设置
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        highlightBuilder.preTags("<em>")
//                .postTags("</em>")
//                .field("name");
        //NativeSearchQueryBuilder 将多个条件组合在一起
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        //组合高亮显示
//        nativeSearchQueryBuilder.withHighlightBuilder(highlightBuilder);
        // 设置分页
        //分页
        PageRequest pageRequest = PageRequest.of(start, size,Sort.by(Sort.Order.asc("id")));
        nativeSearchQueryBuilder.withPageable(pageRequest);
        //组合查询条件
        nativeSearchQueryBuilder.withQuery(query);
        //也可以组合排序条件
//      nativeSearchQueryBuilder.withSort();

        return nativeSearchQueryBuilder.build();
    }

    @PostMapping("/addCategory")
    public String addCategory() throws Exception {
        Category c = new Category();
        int id = currentTime();
        c.setId(id);
        Category save = categoryDao.save(c);
        
        return save.toString();
    }
    private int currentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("MMddHHmmss");
        String time= sdf.format(new Date());
        return Integer.parseInt(time);
    }
 
    @DeleteMapping("/deleteCategory")
    public String deleteCategory(Category c) throws Exception {
        categoryDao.delete(c);
        return "delete ok";
    }
    @PutMapping("/updateCategory")
    public String updateCategory(Category c) throws Exception {
        Category save = categoryDao.save(c);
        return save.toString();
    }
    @RequestMapping("/editCategory")
    public String ediitCategory(int id,Model m) throws Exception {
        Optional<Category> c= categoryDao.findById(id);

        return c.get().toString();
    }
}