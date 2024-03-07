package com.hl.bigdata.spark.scala.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * @author huanglin
 * @date 2023/03/02 21:25
 */
class Spark_Base {
  // 设置日志打印级别
  Logger.getLogger("org").setLevel(Level.WARN);
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN);

  val spark: SparkSession = SparkSession.builder().master("local[4]").appName(this.getClass.getName).getOrCreate();
  val sc: SparkContext = spark.sparkContext;
  sc.setLogLevel("WARN")
}
