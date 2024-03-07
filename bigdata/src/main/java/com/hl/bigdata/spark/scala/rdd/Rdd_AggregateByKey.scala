package com.hl.bigdata.spark.scala.rdd

/**
 * @author huanglin
 * @date 2022/03/26 18:54:56
 */
class Rdd_AggregateByKey extends Rdd_BASE {

  def aggregateByKey(): Unit = {
    val data = List((1, 22), (1, 33), (2, 33), (3, 33), (4, 55));
    val rdd9 = sc.parallelize(data, 3);

    def sum(a: Int, b: Int): Int = {
      return a + b;
    }

    val aggregate_rdd = rdd9.aggregateByKey(0)(sum, sum);
    println(aggregate_rdd.foreach(item => print(item + " ")));
    println("<---------------------------------->");
    val data1 = List((1, 3), (1, 2), (1, 4), (2, 3))
    val rdd10 = sc.parallelize(data, 3);

    //合并不同partition中的值，a，b得数据类型为zeroValue的数据类型
    def combOp(a: String, b: String) = {
      println("combOp: " + a + "\t" + b)
      a + b
    }

    //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型
    def seqOp(a: String, b: Int) = {
      println("SeqOp:" + a + "\t" + b)
      a + b
    }

    //zeroValue:中立值,定义返回value的类型，并参与运算
    //seqOp:用来在同一个partition中合并值
    //combOp:用来在不同partiton中合并值
    val aggregateByKeyRDD = rdd10.aggregateByKey("100")(seqOp, combOp)
    aggregateByKeyRDD.foreach(println);

    println("<---------------------------------->");
    // foldByKey 是 aggregateByKey 的简化操作，seqop 和 combop 相同。注意：V 的类型不能改变。
    val rdd11 = sc.parallelize(List((1, 3), (1, 4), (1, 7), (2, 3), (3, 5), (4, 6), (5,3), (5, 5)), 3);
    val foldByKey = rdd11.foldByKey(0)(_ + _);
    foldByKey.foreach(println);
  }
}

object Rdd_AggregateByKey {

  def main(args: Array[String]): Unit = {
    new Rdd_AggregateByKey().aggregateByKey();
  }
}
