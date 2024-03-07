package com.hl.bigdata.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author huanglin
 * @date 2022/03/13 14:02:20
 */
public class WCApp {

    public static void main(String[] args) {
        SparkConf conf =new SparkConf();
        conf.setMaster("local[4]");
        conf.setAppName("WCApp");

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> rdd = context.textFile("file:/home/huanglin/word.txt");

        JavaRDD<String> word = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> strings = Arrays.asList(s.split(","));
                return strings.iterator();
            }
        });

        JavaPairRDD<String, Integer> counts = word.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        counts.saveAsTextFile("file:/home/huanglin/word_count2");
    }
}
