package com.hl.bigdata.spark.scala.rdd

/**
 * 大表随机添加 N 种随机前缀，小表扩大 N 倍
 * @author huanglin
 * @date 2022/12/10 20:45
 */
class Rdd_Keytoo extends Rdd_BASE {

  val big_rdd = sc.textFile("hdfs://s100:8020/spark/big_test1/")
  val small_rdd = sc.textFile("hdfs://s100:8020/spark/small_test1/");

  def before(): Unit = {
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

  /*如果出现数据倾斜的 Key 比较,此时更适合直接对存在数据倾斜的数据集全部加上随机前缀，然后对另外一个不存在严重数据倾斜的数据集整体与随机前缀集作笛卡尔乘积（即将数据量扩大 N 倍）*/
  def after() : Unit = {
    val small_rdd1 = small_rdd.map(m => {
      val v = m.split("\t");
      (v(0), v(1));
    })

    val big_rdd1 = big_rdd.map(m => {
      val v = m.split("\t");
      (v(0), v(1));
    })
    import scala.collection.mutable._;
    val addList = ArrayBuffer[Int]();
    for (i <- 1 to 24) {
      addList += i;
    }
    val addListKeys = sc.broadcast(addList);

    val big_randomRDD = big_rdd1.map(m => {
      (scala.util.Random.nextInt(48) + "," + m._1, m._2);
    });
    val small_randomRdd = small_rdd1.flatMap(fm => {
      addListKeys.value.toStream.map(m => {
        (m + "," + fm._1, fm._2);
      }).iterator
    })
    big_randomRDD.join(small_randomRdd).map(m => {
      (m._1.split(",")(1), m._2)
    })
  }
}

object Rdd_Keytoo {

  def main(args: Array[String]): Unit = {
    val satrt = System.currentTimeMillis().toInt;
    new Rdd_Keytoo().after();
    val end = System.currentTimeMillis().toInt;
    println(end - satrt);
  }
}
