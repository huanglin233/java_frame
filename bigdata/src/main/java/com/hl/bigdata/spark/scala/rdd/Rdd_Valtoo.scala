package com.hl.bigdata.spark.scala.rdd

/**
 * 为倾斜的 key 增加随机前/后缀
 *
 * @author huanglin
 * @date 2022/12/10 19:23
 */
class Rdd_Valtoo extends Rdd_BASE {

  val big_rdd = sc.textFile("hdfs://s100:8020/spark/big_test1/")
  val small_rdd = sc.textFile("hdfs://s100:8020/spark/small_test1/");
  def random_before() : Unit = {
    val small_rdd1 = small_rdd.map(m => {
      val v = m.split("\t");
      (v(0), v(1));
    })


    val big_rdd1 = big_rdd.map(m => {
      val v = m.split("\t");
      (v(0), v(1));
    })

    val rdd = big_rdd1.join(small_rdd1);
  }

  /*
  * 为数据量特别大的 Key 增加随机前/后缀，使得原来 Key 相同的数据变为 Key 不相同的数据，从而使倾斜的数据集分散到不同的 Task 中，彻底解决数据倾斜问题。
  * Join 另一侧的数据中，与倾斜 Key 对应的部分数据，与随机前缀集作笛卡尔乘积，从而保证无论数据倾斜侧倾斜 Key 如何加前缀，都能与之正常 Join。
  * */
  def random_after(): Unit = {
    val small_rdd1 = small_rdd.map(m => {
      val v = m.split("\t");
      (v(0), v(1));
    })

    val big_rdd1 = big_rdd.map(m => {
      val v = m.split("\t");
      (v(0), v(1));
    })

    import scala.collection.mutable._;
    val skewedKeyArray = ArrayBuffer[Int](33, 55);
    val skewedKeySet = Set[Int]();
    val addList  = ArrayBuffer[Int]();
    for(i <- 1 to 24) {
      addList += i;
    }
    for(i <- skewedKeyArray) {
      skewedKeySet += i;
    }

    val skewedKeys  = sc.broadcast(skewedKeySet);
    val addListKeys = sc.broadcast(addList);

    val big_skewRDD = big_rdd1.filter(f => {
      skewedKeys.value.apply(f._1.toInt);
    }).map(m => {
      ((scala.util.Random.nextInt(24) + 1) + "," + m._1, m._2);
    });
    val small_skewRDD = small_rdd1.filter(f => {
      skewedKeys.value.apply(f._1.toInt);
    }).map(m => {
      ((scala.util.Random.nextInt(24) + 1) + "," + m._1, m._2);
    }).flatMap(fm => {
      addListKeys.value.toStream.map(m => {
        (m + "," + fm._1, fm._2);
      }).iterator
    });

    val skewedJoinRDD = big_skewRDD.join(small_skewRDD, 24).map(m => {
      (m._1.split(",")(1), m._2)
    })

    val big_unSkewRDD = big_rdd1.filter(f => {
      !skewedKeys.value.apply(f._1.toInt);
    }).map(m => {
      ((scala.util.Random.nextInt(24) + 1) + "," + m._1, m._2);
    });
    val unSkewedJoinRDD = big_unSkewRDD.join(small_rdd1);
    val rdd = skewedJoinRDD.union(unSkewedJoinRDD);
  }
}
object Rdd_Valtoo {

  def main(args: Array[String]): Unit = {
    val satrt = System.currentTimeMillis().toInt;
    new Rdd_Valtoo().random_after();
    val end = System.currentTimeMillis().toInt;
    println(end - satrt);
  }
}
