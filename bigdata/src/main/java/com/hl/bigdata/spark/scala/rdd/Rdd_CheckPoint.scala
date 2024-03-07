package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/27 14:34:28
 */
class Rdd_CheckPoint extends Rdd_BASE {

  def checkPoint(): Unit = {
    val rdd = sc.makeRDD(1 to 30, 3);
    // 设置检查点位置
    sc.setCheckpointDir("hdfs://s100:8020/user/spark/checkPoint");
    println(rdd.count());
    rdd.checkpoint();
    val rdd_map1 = rdd.map(_.toString + "[" + System.currentTimeMillis() + "]");
    val rdd_map2 = rdd.map(_.toString + "[" + System.currentTimeMillis() + "]");
    rdd_map1.checkpoint();

    println(rdd.collect().foreach(print));
    println(rdd.collect().foreach(print));
    println(rdd_map1.collect().foreach(print));
    println(rdd_map1.collect().foreach(print));
    println(rdd_map1.collect().foreach(print));
    println(rdd_map2.collect().foreach(print));
    println(rdd_map2.collect().foreach(print));
    println(rdd_map2.collect().foreach(print));
  }
}

object Rdd_CheckPoint {

  def main(args: Array[String]): Unit = {
    new Rdd_CheckPoint().checkPoint();
  }
}
