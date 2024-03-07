package com.hl.bigdata.spark.scala.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author huanglin
 * @date 2022/03/26 18:31:09
 */
class Rdd_BASE {

  // 设置日志打印级别
  Logger.getLogger("org").setLevel(Level.WARN);
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("WordCount").setMaster("local[3]");
  val sc = new SparkContext(conf);

  sc.setLogLevel("WARN");
}
