package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/27 13:34:41
 */
class Rdd_Math extends Rdd_BASE {

  def math(): Unit = {
    val rdd = sc.makeRDD(1 to 20);
    println(rdd.count());
    // 平均数
    println(rdd.mean());
    println(rdd.sum());
    println(rdd.max());
    println(rdd.min());
    // 元素方差
    println(rdd.variance());
    // 从采样中计算出的方差
    println(rdd.sampleVariance());
    // 标准差
    println(rdd.stdev());
    // 采样的标准差
    println(rdd.sampleStdev());
  }
}

object Rdd_Math {

  def main(args: Array[String]): Unit = {
    new Rdd_Math().math();
  }
}
