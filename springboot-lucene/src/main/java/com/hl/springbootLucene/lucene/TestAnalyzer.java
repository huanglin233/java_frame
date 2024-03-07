package com.hl.springbootLucene.lucene;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class TestAnalyzer {

    public static void main(String[] args) throws IOException {

        try (IKAnalyzer analyzer = new IKAnalyzer()) {
            TokenStream ts       = analyzer.tokenStream("name", "护眼带光源");
            ts.reset();
            while (ts.incrementToken()) {
                System.out.println(ts.reflectAsString(false));
            }
        }
    }
}
