package com.hl.bigdata.spark.scala.rdd

/**
 * 将 Reduce side Join 转变为 Map side Join
 *
 * @author huanglin
 * @date 2022/12/08 21:36
 */
class Rdd_Broadcast extends Rdd_BASE {

  def broadcast_before() : Unit = {
    val sc_rdd_big = sc.textFile("hdfs://s100:8020/spark/test/big_data.txt")
    val sc_rdd_small = sc.textFile("hdfs://s100:8020/spark/test/small_data.txt");
    val join_big = sc_rdd_big.map(x => {
      val param = x.split(",");
      (param(0).trim().toInt, param(1).trim);
    })
    val join_small = sc_rdd_small.map(x => {
      val param = x.split(",");
      (param(0).trim().toInt, param(1).trim);
    })

    join_big.join(join_small).count();
  }

  def broadcast_after() : Unit = {
    val sc_rdd_big = sc.textFile("hdfs://s100:8020/spark/test/big_data.txt")
    val sc_rdd_small = sc.textFile("hdfs://s100:8020/spark/test/small_data.txt");
    val join_big = sc_rdd_big.map(x => {
      val param = x.split(",");
      (param(0).trim().toInt, param(1).trim);
    })
    val join_small = sc_rdd_small.map(x => {
      val param = x.split(",");
      (param(0).trim().toInt, param(1).trim) ;
    })

    val broadcastVal = sc.broadcast(join_small.collectAsMap()); // #把分散的 RDD 转换为 Scala 的集合类型
    join_big.map(x => {
      (x._1, (x._2, broadcastVal.value.getOrElse(x._1, "")))
    }).count;
  }
}
