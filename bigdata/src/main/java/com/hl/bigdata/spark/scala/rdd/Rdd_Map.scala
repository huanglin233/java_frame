package com.hl.bigdata.spark.scala.rdd

import scala.collection.mutable.ArrayBuffer

/**
 * @author huanglin
 * @date 2022/03/26 18:42:20
 */
class Rdd_Map extends Rdd_BASE {

  def map(): Unit = {
    val rdd = sc.textFile("file:/home/huanglin/spark_log/log.txt");
    // map方法
    println("<---------------------------------->");
    val flatMap_rdd1 = rdd.flatMap(_.split(","));
    // map函数是一条数据一条数据的处理，也就是，map的输入参数中要包含一条数据以及其他你需要传的参数
    val map_rdd = flatMap_rdd1.map(word => {
      val m = (word, 1);
      m;
    });
    println(map_rdd.foreach(print));
    println("<---------------------------------->");
    // mapPartitions函数是一个partition数据一起处理，也即是说，mapPartitions函数的输入是一个partition的所有数据构成的“迭代器”，然后函数里面可以一条一条的处理，在把所有结果，按迭代器输出
    val par_rdd = flatMap_rdd1.mapPartitions(item => {
      val buf = ArrayBuffer[String]();
      val thread_name = Thread.currentThread().getName;
      println(thread_name + " : " + "mapPartitions start");
      for (e <- item) {
        buf.+=("_" + e);
      }
      buf.iterator;
    });
    println(par_rdd.foreach(print));
    println("<---------------------------------->");
    // mapPartitionsWithIndex函数，其实和mapPartitions函数区别不大，因为mapPartitions背后调的就是mapPartitionsWithIndex函数，只是一个参数被close了。mapPartitionsWithIndex的函数可以或得partition索引号
    val parIndex_rdd = flatMap_rdd1.mapPartitionsWithIndex((index, item) => {
      val buf = ArrayBuffer[String]();
      val thread_name = Thread.currentThread().getName;
      println(thread_name + " : " + index + " mapPartitions start");
      for (e <- item) {
        buf.+=("_" + e)
      }
      buf.iterator
    });
    println(parIndex_rdd.foreach(print));
    println("<---------------------------------->");
    val rdd2 = sc.makeRDD(Array((1, "a"), (1, "d"), (2, "c"), (3, "b")));
    rdd2.mapValues(_ + "|").collect().foreach(println);
  }
}

object Rdd_Map {

  def main(args: Array[String]): Unit = {
    new Rdd_Map().map();
  }
}

