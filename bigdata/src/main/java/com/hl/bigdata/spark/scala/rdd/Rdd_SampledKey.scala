package com.hl.bigdata.spark.scala.rdd

import org.apache.spark.network.sasl.ShuffleSecretManager
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * 采样倾斜 key 并分拆 join 操作
 *
 * @author huanglin
 * @date 2022/12/10 21:21
 */
class Rdd_SampledKey extends Rdd_BASE {

  val big_rdd = sc.textFile("hdfs://s100:8020/spark/big_test1/")
  val small_rdd = sc.textFile("hdfs://s100:8020/spark/small_test1/");
  val small_rdd1 = small_rdd.map(m => {
    val v = m.split("\t");
    (v(0), v(1));
  })
  val big_rdd1 = big_rdd.map(m => {
    val v = m.split("\t");
    (v(0), v(1));
  })
  def before() : Unit = {
    val rdd = big_rdd1.join(small_rdd1);
  }

  /**
   * 两个 RDD/Hive 表进行 join 的时候，如果数据量都比较大，那么此时可以看一下两个 RDD/Hive 表中的 key 分布情况。如果出现数据倾斜，
   * 是因为其中某一个 RDD/Hive 表中的少数几个 key 的数据量过大，而另一个 RDD/Hive 表中的所有 key 都分布比较均匀，那么采用这个解决方案是比较合适的。
   */
  def after() : Unit = {
    // 首先从包含了少数几个导致数据倾斜 key 的 big_rdd1 中，采样 10% 的样本数据
    val sampledRDD = big_rdd1.sample(false, 0.1);
    // 对样本数据 RDD 统计出每个 key 的出现次数，并按出现次数降序排序。
    // 对降序排序后的数据，取出 top 1 或者 top 100 的数据，也就是 key 最多的前 n 个数据。
    val mappedSampledRDD  = sampledRDD.map(m => {
      (m, 1L);
    });
    val countedSampledRDD = mappedSampledRDD.reduceByKey((rb1, rb2) => {
      (rb1 + rb2);
    });
    val reversedSampledRDD = countedSampledRDD.map(m => {
      (m._2, m._1);
    });
    // 取出数据两个最多个key
    val skewedUserid1 = reversedSampledRDD.sortByKey(false).take(2)(0)._2;
    val skewedUserid2 = reversedSampledRDD.sortByKey(false).take(2)(1)._2;
    // 从 big_rdd1 中分拆出导致数据倾斜的 key，形成独立的 RDD。
    val big_skewedRDD1 = big_rdd1.filter(f => {
      f._1.equals(skewedUserid1);
    });
    val big_skewedRDD2 = big_rdd1.filter(f => {
      f._1.equals(skewedUserid2);
    });
    // 从 big_rdd1 中分拆出不导致数据倾斜的普通 key，形成独立的 RDD。
    val commonRDD = big_rdd1.filter(f => {
      !f._1.equals(skewedUserid1) && !f._1.equals(skewedUserid2);
    });
    // small_rdd1，就是那个所有 key 的分布相对较为均匀的 rdd。
    // 这里将 small_rdd1 中，前面获取到的 key 对应的数据，过滤出来，分拆成单独的 rdd，并对 rdd 中的数据使用 flatMap 算子都扩容 100 倍。
    // 对扩容的每条数据，都打上 0～100 的前缀。
    val list = ArrayBuffer[Int]()
    for (i <- 1 to 100) {
      list += i;
    }
    val small_skewedRDD1 = small_rdd1.filter(f => {
      f._1.equals(skewedUserid1);
    }).flatMap( fm =>{
      list.toStream.map(m => {
        (scala.util.Random.nextInt(100) + "_" + fm._1, fm._2);
      }).iterator
    });

    val small_skewedRDD2 = small_rdd1.filter(f => {
      f._1.equals(skewedUserid1);
    }).flatMap( fm => {
      list.toStream.map(m => {
        (scala.util.Random.nextInt(100) + "_" + fm._1, fm._2);
      }).iterator
    });
    // 将 big_rdd1 中分拆出来的导致倾斜的 key 的独立 rdd，每条数据都打上 100 以内的随机前缀。
    // 然后将这个 big_rdd1 中分拆出来的独立 rdd，与上面 small_rdd1 中分拆出来的独立 rdd，进行 join。
    val joinedRDDKey1 = big_skewedRDD1.map(m => {
      (scala.util.Random.nextInt(100) + "_" + m._1, m._2);
    }).join(small_skewedRDD1).map(m => {
      (m._1.split("_")(1), m._2);
    });
    val joinedRDDKey2 = big_skewedRDD2.map(m => {
      (scala.util.Random.nextInt(100) + "_" + m._1, m._2);
    }).join(small_skewedRDD2).map(m => {
      (m._1.split("_")(1), m._2);
    });

    // 将 big_rdd1 中分拆出来的包含普通 key 的独立 rdd，直接与 small_rdd1 进行 join。
    val commonJoin = commonRDD.join(small_rdd1);
    val re = commonJoin.union(joinedRDDKey1).union(joinedRDDKey2);
  }
}

object Rdd_SampledKey {

  def main(args: Array[String]): Unit = {
    val satrt = System.currentTimeMillis().toInt;
    new Rdd_SampledKey().after();
    val end = System.currentTimeMillis().toInt;
    println(end - satrt);
  }
}
