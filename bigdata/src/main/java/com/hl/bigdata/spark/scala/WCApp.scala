package com.hl.bigdata.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author huanglin
 * @date 2022/03/13 12:36:09
 */
object WCApp {

  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local");
    val sc = new SparkContext(conf);
    val lines = sc.textFile("file:/home/huanglin/word.txt");
    val words = lines.flatMap(x => x.split(","));
    val counts = words.map(w => (w, 1)).reduceByKey((x, y) => x + y);
    counts.saveAsTextFile("file:/home/huanglin/word_count1");
  }
}
