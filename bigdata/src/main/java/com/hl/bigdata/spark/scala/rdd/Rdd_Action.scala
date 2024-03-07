package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/26 21:39:16
 */
class Rdd_Action extends Rdd_BASE {

  def action(): Unit = {
    val rdd = sc.makeRDD(1 to 20, 2);
    println(rdd.reduce(_ + _));
    // 返回一个数组，该数组由从数据集中随机采样的 num 个元素组成，可以选择是否用随机数替换不足的部分，seed 用于指定随机数生成器种子。
    println(rdd.takeSample(true, 5, 3).foreach(println));
    // 返回前几个的排序
    println(rdd.top(2).foreach(println));
    println(rdd.takeOrdered(2).foreach(println));
    // 操作的是数值型数据，aggregate 函数将每个分区里面的元素通过 seqOp 和初始值进行聚合，然后用 combine 函数将每个分区的结果和初始值 (zeroValue) 进行 combine 操作。这个函数最终返回的类型不需要和 RDD 中元素类型一致。
    // def aggregate(zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)
    println(rdd.aggregate(2)((_ + _), (_ + _)));
  }
}

object Rdd_Action {

  def main(args: Array[String]): Unit = {
    new Rdd_Action().action();
  }
}
