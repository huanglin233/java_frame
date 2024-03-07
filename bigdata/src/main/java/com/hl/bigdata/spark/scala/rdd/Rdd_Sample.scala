package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/26 18:25:11
 */
class Rdd_Sample extends Rdd_BASE {

  def sample(): Unit = {
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8 , 9, 10, 11, 12, 13));
    val sample_rdd = rdd.sample(true, 0.5, 3);
    sample_rdd.collect().foreach(println);
  }
}

object Rdd_Sample {
  def main(args: Array[String]): Unit = {
    new Rdd_Sample().sample();
  }
}
