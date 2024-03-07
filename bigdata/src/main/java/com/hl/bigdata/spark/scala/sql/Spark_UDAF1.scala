package com.hl.bigdata.spark.scala.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator


/**
 *  通过继承Average来实现强类型自定义聚合函数
 *
 * @author huanglin
 * @date 2023/03/07 22:11
 */
case class People(age: Long,name: String, sex: String);
case class Average(var sum : Long, var count: Long)
// 其中 People 是在应用聚合函数的时候传入的对象，Average 是聚合函数在运行的时候内部需要的数据结构，Double 是聚合函数最终需要输出的类型
class Spark_UDAF1 extends Aggregator[People, Average, Double]{

  // 定义一个数据结构,保存年龄总数和人员总个数,初始都为0
  override def zero: Average = Average(0, 0);

  // 相同Execute间的数据合并(同一分区)
  override def reduce(b: Average, a: People): Average = {
    b.sum += a.age;
    b.count += 1;
    b;
  }

  // 聚合不同Execute
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count;
    b1
  }

  // 计算最终结果
  override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  // 设置之间值类型的编码器,要转化成case类
  // Encoders.product 是进行 scala 元组和 case 类转换的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product

  // 设置最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object Spark_UDAF1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN);

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName(this.getClass.getName).getOrCreate();
    val sc: SparkContext = spark.sparkContext;
    sc.setLogLevel("WARN");
    import spark.implicits._

    val df = spark.read.json(sc.wholeTextFiles("/user/spark/spark_json2.json").values).as[People];
    df.show();
    val averageAge = new Spark_UDAF1().toColumn.name("average_age");
    val result = df.select(averageAge);
    result.show();

    spark.stop();
  }
}