package com.hl.bigdata.spark.scala.rdd

import scala.util.Random

/**
 * 两阶段聚合（局部聚合+全局聚合）
 * 对 RDD 执行 reduceByKey 等聚合类 shuffle 算子或者在 Spark SQL 中使用 group by 语句进行分组聚合时，比较适用这种方案。
 *
 * @author huanglin
 * @date 2022/12/08 22:35
 */
class Rdd_ReduceByKey extends Rdd_BASE {

  def map_reduce_map_reduce(): Unit = {
    val rdd = sc.textFile("hdfs://s100:8020/spark/test/big_data.txt");
    // 第一步，给 RDD 中的每个 key 都打上一个随机前缀。
    val rdd_map1 = rdd.map(m => {
      val random = Random.nextInt(100);
      val s = m.split(",");
      (random + "_" + s(0), s(1));
    })
    // 第二步，对打上随机前缀的 key 进行局部聚合。
    val rdd_reduce1 = rdd_map1.reduceByKey((v1,v2) => {
      v1 + v2;
    })
    // 第三步，去除 RDD 中每个 key 的随机前缀。
    val rdd_map2 = rdd_reduce1.map(m => {
      val key = m._1.split("_")(1);
      (key, m._2)
    })
    // 第四步，对去除了随机前缀的 RDD 进行全局聚合。
    val rdd_reduce2 = rdd_map2.reduceByKey((v1, v2) => {
      v1 + v2;
    })

    rdd_reduce2.groupByKey().count();
  }
}
