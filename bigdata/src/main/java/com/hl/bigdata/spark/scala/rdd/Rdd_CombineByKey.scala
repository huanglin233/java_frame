package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/26 19:59:04
 */
class Rdd_CombineByKey extends Rdd_BASE {

  def combineByKey(): Unit = {
    // combineByKey
    val data3 = List(("Jack", 88.0), ("Tom", 90.0), ("Jim", 88.0), ("Jack", 98.0), ("Tom", 91.0), ("Jim", 68.0));
    val rdd13 = sc.parallelize(data3);
    type Type_ = (Int, Double);
    val combineByKey_rdd = rdd13.combineByKeyWithClassTag(a  => (1, a), (b : Type_, c) => (b._1 + 1, b._2 + c), (d : Type_, e : Type_) => (d._1 + e._1, d._2 + e._2)).map{case (name, (num, scores)) => (name, scores / num)}.collect();
    println(combineByKey_rdd.foreach(println));
  }
}

object Rdd_CombineByKey {

  def main(args: Array[String]): Unit = {
    new Rdd_CombineByKey().combineByKey();
  }
}
