package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/26 18:02:51
 */
class Rdd_Cartesian extends Rdd_BASE {

  def cartesian(): Unit = {
    val rdd7 = sc.parallelize(Seq("tom", "jack", "jim", "mary"));
    val rdd8 = sc.parallelize(Seq("123", "jack", "s3123", "231", "jim"));
    // 笛卡尔
    val cartesian_rdd = rdd7.cartesian(rdd8);
    cartesian_rdd.collect().foreach(item => print(item + ""));
  }
}

object Rdd_Cartesian {

  def main(args: Array[String]): Unit = {
    new Rdd_Cartesian().cartesian();
  }
}
