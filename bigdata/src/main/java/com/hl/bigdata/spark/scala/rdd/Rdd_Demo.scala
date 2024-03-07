package com.hl.bigdata.spark.scala.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author huanglin
 * @date 2022/03/27 16:24:14
 */
case class Rdd_Demo(timestamp : Long, province : Int, city : Int, userId : Int, adId : Int) {

  /**
   * timestamp   province    city        userid      adid
   * 某个时间点  某个省份    某个城市    某个用户    某个广告
   *
   * 用户 ID 范围： 0 - 99
   * 省份、城市 ID 相同： 0 - 9
   * adid范围：0 - 19
   *
   * 需求1：统计每一个省份点击 TOP3 的广告 ID
   * 需求2：统计每一个省份每一个小时点击 TOP3 广告的 ID
   */
}

object  Rdd_Demo extends Rdd_BASE {

  def main(args: Array[String]): Unit = {
    val cur_path = this.getClass.getResource("/");
    val rdd = sc.textFile(cur_path + "demo_agent.text");
    val demo_rdd : RDD[Rdd_Demo] = rdd.map(item => {
      val strs = item.split(" ");
      Rdd_Demo(strs(0).toLong, strs(1).toInt, strs(2).toInt, strs(3).toInt, strs(4).toInt);
    });
    demo_rdd.cache();

    // 统计每一个省份点击 TOP3 的广告 ID，先创建一个最小粒度
    val count1_rdd = demo_rdd.map(item => {
      (item.province + "_" + item.adId, 1)
    })
    val count2_rdd = count1_rdd.reduceByKey(_ + _);
    // 逐渐放大粒度
    val count3_rdd = count2_rdd.map(item => {
      val p = item._1.split("_");
      (p(0).toInt, (p(1).toInt, item._2));
    });
    val count4_rdd = count3_rdd.groupByKey();
    val res1_rdd = count4_rdd.mapValues(_.toList.sortWith(_._2 > _._2).take(3));
    println(res1_rdd.collect().foreach(println));

    println("<---------------------------------->");
    // 需求2：统计每一个省份每一个小时点击 TOP3 广告的 ID
    val sf = new SimpleDateFormat("YYYY-MM-dd HH");
    val count5_rdd = demo_rdd.map(item => (item.province + "_" + sf.format(new Date(item.timestamp)) + "_" + item.adId, 1));
    val count6_rdd : RDD[(String, Int)] = count5_rdd.reduceByKey(_ + _);
    val res2_rdd = count6_rdd.map(item => {
      val p = item._1.split("_");
      (p(0) + "_" + p(1), (p(2), item._2));
    }).groupByKey().mapValues(_.toList.sortWith(_._2 > _._2).take(3)).map(item => {
      val p = item._1.split("_");
      (p(0).toInt, (p(1), item._2));
    }).groupByKey();
    println(res2_rdd.collect().foreach(println));

    sc.stop();
  }
}
