package com.hl.bigdata.spark.scala.sql

import com.hl.bigdata.spark.scala.rdd.Rdd_BASE
import org.apache.spark.sql.SparkSession

/**
 * @author huanglin
 * @date 2023/02/23 22:02
 */
class Spark_Json extends Rdd_BASE{
  def readJson1(): Unit = {
    // 创建SparkSession并设置App名称
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("Spark.some.config.option", "some-value")
      .getOrCreate();

    // 通过隐式转换将RDD操作添加到DataFrame上
    import spark.implicits._
    // 通过spark.read操作读取JSON数据

    val df = spark.read.json(sc.wholeTextFiles("/user/spark/spark_json2.json").values);
    // show操作类似于Action,将DataFrame直接打印到Console上
    df.show();
    // DSL风格的使用方式: 属性的获取方法$
    df.filter($"age" > 19).show();
    // 将DataFrame注册为表
    df.createOrReplaceTempView("persons");
    // 执行spark sql查询操作
    val ss = spark.sql("select * from persons where age > 20").show();
    // 关闭资源
    spark.stop();
  }
}

object Spark_Json {

  def main(args: Array[String]): Unit = {
    new Spark_Json().readJson1();
  }
}