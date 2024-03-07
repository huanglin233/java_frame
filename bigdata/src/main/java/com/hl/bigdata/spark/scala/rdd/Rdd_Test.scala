package com.hl.bigdata.spark.scala.rdd

import com.hl.bigdata.spark.scala.customObj.CustomPartitioner
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author huanglin
 * @date 2022/03/13 15:02:17
 */
class Rdd_Test extends Rdd_BASE {

  def test(): Unit = {
    val rdd = sc.textFile("file:/home/huanglin/spark_log/log.txt");
    // 获取error行
    val error_rdd = rdd.filter(_.toLowerCase.contains("error"));
    // 获取warn行
    val warn_rdd = rdd.filter(_.toLowerCase.contains("warn"));
    // 并集
    val all_rdd = error_rdd.union(warn_rdd);
    // 交集
    all_rdd.collect().foreach(println);
    println("<---------------------------------->");
    val intersec_rdd = warn_rdd.intersection(error_rdd);
    println(intersec_rdd.foreach(println));
    // 计算差的一种函数，去除两个 RDD 中相同的元素，不同的 RDD 将保留下来。举例说明：A - B = A - A 与 B 的交集, 求差集
    println("<---------------------------------->");
    warn_rdd.subtract(error_rdd).collect().foreach(println);
    // 执行sh脚本
    println("<---------------------------------->");
    val pip_rdd = rdd.pipe("ls -al /soft");
    pip_rdd.collect().foreach(println);
    val pipTest = sc.parallelize(List("hello", "Jim", "are", "you", "ok"))
    val pip_rdd2 = pipTest.pipe("/home/huanglin/spark_log/pipe.sh");
    pip_rdd2.collect().foreach(println);
  }
}

object Rdd_Test{

  def main(args: Array[String]): Unit = {
   new Rdd_Test().test();
  }
}
