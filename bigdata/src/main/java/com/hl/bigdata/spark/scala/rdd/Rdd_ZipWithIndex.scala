package com.hl.bigdata.spark.scala.rdd

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * 给原始数据增加 ID 属性
 * 将 RDD 通过 zipWithIndex 实现 ID 添加，将 RDD 以制表符分割并转换为 ArrayBuffer，然后通过 mkString 将数据以 Text 输出。
 *
 * @author huanglin
 * @date 2022/10/06 17:22
 */

case class brower(id: Int, time: Long, uid: String, keyWord: String, url_rank: String, click_num: Int, click_url: String, other: String) extends Serializable;
class Rdd_ZipWithIndex{
  val sparkSession = SparkSession.builder().appName("RddToDataset").master("local").getOrCreate();
  val sc = sparkSession.sparkContext;

  def do_ZipWithIndex() : Unit = {
    val rdd = sc.textFile("hdfs://s100:8020/spark/test/20220808.txt");
    val rdd_ZipWithIndex = rdd.zipWithIndex().map(tuple => {
      val array = new ArrayBuffer[String]();
      array++= (tuple._1.split("\t")); // 集合合并,改变左值
      tuple._2.toString +=: array; // 单值插入头部
      array.toArray;
    })

    rdd_ZipWithIndex.map(_.mkString("\t")).saveAsTextFile("hdfs://s100:8020/spark/test/20221006_source_index_1");
  }

  def load_ZipWithIndex() : Unit = {
    val rdd = sc.textFile("hdfs://s100:8020/spark/test/20221006_source_index_1/part-00000");
    // 创建一个case类代表数据集

    import  sparkSession.implicits._
    val ds = rdd.map(_.split("\t")).map(
      attr =>{
        brower(attr(0).toInt, attr(1).toLong, attr(2), attr(3), attr(4), attr(5).toInt, attr(6), attr(7))
      }
    ).toDS();
    ds.createTempView("sourceTable");
    val newSource = sparkSession.sql("select case when id < 900000 then (8 + (cast (rand() * 50000 as bigint)) * 12 ) else id end, time, uid, keyWord, url_rank, click_num, click_url, other from sourceTable");
    newSource.rdd.map(_.mkString("\t")).saveAsTextFile("hdfs://s100:8020/spark/test/20221006_source_index_2");
  }
}

object Rdd_ZipWithIndex {
  def main(args: Array[String]): Unit = {
//    new Rdd_ZipWithIndex().do_ZipWithIndex();
    new Rdd_ZipWithIndex().load_ZipWithIndex();
  }
}
