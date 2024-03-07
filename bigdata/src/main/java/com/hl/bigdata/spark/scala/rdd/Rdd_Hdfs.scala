package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/26 19:27:20
 */
class Rdd_Hdfs extends Rdd_BASE {

  def hdfs(): Unit = {
    val rdd = sc.textFile("hdfs://s100:8020/user/MR/wc.txt");
    println(rdd.foreach(println));
    val rdd2 = sc.makeRDD(List((1, "Jack"), (2, "tom"), (2, "tom2"), (3, "jim")));
    val rdd3 = rdd2.map((1, _)).groupByKey();
    rdd3.saveAsTextFile("hdfs://s100:8020/user/spark/rdd.txt");
  }
}

object Rdd_Hdfs {

  def main(args: Array[String]): Unit = {
    new Rdd_Hdfs().hdfs();
  }
}
