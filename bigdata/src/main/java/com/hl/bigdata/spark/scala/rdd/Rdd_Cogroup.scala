package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/26 18:37:34
 */
class Rdd_Cogroup extends Rdd_BASE {

  def cogroup(): Unit = {
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

    // When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples
    val coGroup = names_rdd2.cogroup(scores_rdd2);
    println(coGroup.foreach(println));
    coGroup.collect().foreach(item => {
      print(item._1 + " : ");
      for (e <- item._2._1) {
        print(" " + e + " ");
      }
      for (e <- item._2._2) {
        print(" " + e + " ");
      }
      println();
    })
    println("<---------------------------------->");
    scores_rdd1.take(3).foreach(println);
  }
}

object Rdd_Cogroup {

  def main(args: Array[String]): Unit = {
    new Rdd_Cogroup().cogroup();
  }
}
