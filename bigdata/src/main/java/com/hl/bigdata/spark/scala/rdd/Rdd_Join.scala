package com.hl.bigdata.spark.scala.rdd

import scala.util.Random

/**
 * @author huanglin
 * @date 2022/03/26 18:02:31
 */
class Rdd_Join extends Rdd_BASE {

  def join(): Unit = {
    val names_rdd1 = sc.textFile("file:/home/huanglin/spark_log/names.txt");
    val names_rdd2 = names_rdd1.map(line => {
      val arr = line.split(",");
      (arr(0).toLong, arr(1));
    })
    val scores_rdd1 = sc.textFile("file:/home/huanglin/spark_log/scores.txt");
    val scores_rdd2 = scores_rdd1.map(line => {
      val arr = line.split(",");
      (arr(0).toLong, arr(1));
    })
    val join_rdd = names_rdd2.join(scores_rdd2);
    println(join_rdd.foreach(println));
    println("<---------------------------------->");
    val leftJoin_rdd = names_rdd2.leftOuterJoin(scores_rdd2);
    println(leftJoin_rdd.foreach(println));
    println("<---------------------------------->");
    val fullOutJoin_rdd = names_rdd2.fullOuterJoin(scores_rdd2);
    println(fullOutJoin_rdd.foreach(println));
  }

  def join2(): Unit = {
    val rdd1 = sc.parallelize(List(("1", "jack"), ("4", "jim"), ("3", "tom"), ("2", "mary")));
    val rdd2 = sc.parallelize(List(("1", "17"), ("4", "23"), ("3", "33"), ("2", "44"), ("3", "x3"), ("2", "4x")));
    val rdd11 = rdd1.sortByKey();
    val rdd22 = rdd2.sortByKey();
    val join_rdd = rdd11.join(rdd22);
    println(join_rdd.foreach(println));
    println("<---------------------------------->");
    val m_rdd = join_rdd.map(m => {
      (m._1, m._2._2);
    })
    println(m_rdd.foreach(println));
    println("<---------------------------------->");
    val map_rdd = rdd1.collectAsMap();
    map_rdd.values.foreach(x => println(x));
    println("<---------------------------------->");
    val x = rdd1.collect();
  }
}

object Rdd_Join {

  def main(args: Array[String]): Unit = {
//    new Rdd_Join().join();
    new Rdd_Join().join2();
  }
}
