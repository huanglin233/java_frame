package com.hl.springbootELK.boot;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface CategoryDao extends ElasticsearchRepository<Category, Integer>{

}
