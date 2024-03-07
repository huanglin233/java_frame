package com.hl.bigdata.spark.scala.sql

/**
 * 用户自定义UDF函数
 * @author huanglin
 * @date 2023/03/04 19:06
 */
class Spark_UDF extends Spark_Base {
  import spark.implicits._

  def udf1() : Unit = {
    val df = spark.read.json(sc.wholeTextFiles("/user/spark/spark_json2.json").values);
    df.show();
    spark.udf.register("addName", (x : String) => "Name:" + x);
    df.createOrReplaceTempView("people");
    spark.sql("select addName(name), age from people").show();
    spark.sql("select addName(name) as newName, age from people").show();
  }
}

object Spark_UDF {

  def main(args: Array[String]): Unit = {
    new Spark_UDF().udf1();
  }
}
