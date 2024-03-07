package com.hl.bigdata.spark.scala.rdd

import scala.util.Random

/**
 * @author huanglin
 * @date 2022/03/26 18:13:42
 */
class Rdd_ByKey extends Rdd_BASE {

  def byKey(): Unit = {
    val rdd1 = sc.parallelize(Seq((1, 22), (1, 33), (2, 33), (3, 33), (4, 55)));
    val groupBy_rdd = rdd1.groupByKey();
    groupBy_rdd.collect().foreach(item => {
      val key = item._1;
      print(key + " : ");
      for (e <- item._2) {
        print(e + " ")
      }
      println();
    });
    println("<---------------------------------->");
    val rdd3 = sc.textFile("file:/home/huanglin/spark_log/log.txt");
    val flatMap_rdd2 = rdd3.flatMap(_.split(","));
    println(flatMap_rdd2 foreach (item => print(" " + item)));
    // 去重
    val distinct_rdd = flatMap_rdd2.distinct();
    println(distinct_rdd.foreach(item => print(" " + item)));
    val fm_rdd = flatMap_rdd2.map((_, 1)).map(item => {
      val word = item._1;
      val r = Random.nextInt(100);
      (word + "_" + r, 1);
    }).reduceByKey(_ + _, 4).map(item => {
      val word = item._1;
      val count = item._2;
      val w = word.split("_")(0);
      (w, count);
    }).reduceByKey(_ + _, 4);
    println(fm_rdd.foreach(item => println(item + " ")));
    println("<---------------------------------->");
    // 对数据进行排序
    val rdd11 = sc.parallelize(List(("1", "jack"), ("4", "jim"), ("3", "tom"), ("2", "mary")));
    implicit val sortStringByInteger = new Ordering[String] {
      override def compare(a: String, b: String): Int = a.toInt.compare(b.toInt)
    }
    val sort_rdd = rdd11.sortByKey();
    println(sort_rdd.foreach(println));
    println("<---------------------------------->");
    val rdd12 = sc.parallelize(List((1, 2), (1, 3), (2, 3)));
    val countKey_rdd = rdd12.countByKey();
    println(countKey_rdd.foreach(print));
    val map_rdd3 = rdd12.collectAsMap();
    println(map_rdd3.foreach(print));
    // 找出相同的key的value
    println("<---------------------------------->");
    val lookup_rdd = rdd12.lookup(1);
    println(lookup_rdd.foreach(print));
  }

  def byKey2() : Unit = {
    val words = Array("one", "two", "two", "three", "three", "three");
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1));

    val wordCountWithReduce = wordPairsRDD.reduceByKey(_ + _).collect();
    println(wordCountWithReduce.foreach(x => print(x + " ")));

    val wordCountWithGroup = wordPairsRDD.groupByKey().map(x => (x._1, x._2.sum)).collect();
    println(wordCountWithGroup.foreach(x => print(x + " ")));
  }
}

object Rdd_ByKey {

  def main(args: Array[String]): Unit = {
    new Rdd_ByKey().byKey2();
  }
}