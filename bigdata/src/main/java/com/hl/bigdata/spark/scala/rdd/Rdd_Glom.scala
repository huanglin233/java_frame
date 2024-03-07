package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/26 21:21:06
 */
class Rdd_Glom extends Rdd_BASE {

  def glom(): Unit = {
    val rdd = sc.parallelize(1 to 10, 3);
    // 将每一个分区形成一个数组，形成新的 RDD 类型是 RDD[Array[T]]。
    rdd.glom().collect().foreach(x => println(x.foreach(print(_))));
  }
}

object Rdd_Glom {

  def main(args: Array[String]): Unit = {
    new Rdd_Glom().glom();
  }
}
