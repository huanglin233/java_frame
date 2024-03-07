package com.hl.bigdata.spark.scala.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @author huanglin
 * @date 2023/03/02 22:12
 */

case class Person(name : String, age : Int);
class Spark_Dataframe extends Spark_Base {
  import spark.implicits._

  def schema() : Unit = {
    val peopleDF = spark.sparkContext.textFile("/user/spark/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0).trim(), attributes(1).trim().toInt))
      .toDF();
    peopleDF.show();

    peopleDF.createOrReplaceTempView("people");
    val teenagersDF = spark.sql("select name, age from people where age between 20 and 21");
    teenagersDF.show();
    teenagersDF.map(m => "Name: " + m(0)).show(); // 通过 row 对象的索引进行访问

    // or by field name 通过row对象的getAs方法访问
    teenagersDF.map(m => "Name: " + m.getAs[Int](0)).show(); // 以索引访问
    teenagersDF.map(m => "age: " + m.getAs[String]("age")).show(); // 以列名访问

    //(没有为数据集 [Map [K，V]] 预定义的编码器明确定义
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]];
    val coll = teenagersDF.map(m => m.getValuesMap[Any](List("name", "age"))).collect();
    coll.foreach(x => println(x));
  }

  def schema1(): Unit = {
    val peopleRDD = spark.sparkContext.textFile("/user/spark/people.txt")
    val schemaString = "name age";
    // 把name和age都设置成StringType类型
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true));
    val schema = StructType(fields);
    val rowRDD = peopleRDD.map(_.split(","))
      .map(attributes => Row(attributes(0).trim, attributes(1).trim));
    val peopleDF = spark.createDataFrame(rowRDD, schema);
    peopleDF.createOrReplaceTempView("people");
    val results = spark.sql("select name, age from people where age between 20 and 21");
    results.map(attributes => "Name: " + attributes(0) + ";age: " + attributes.get(1)).show();
  }
}

object Spark_Dataframe {

  def main(args: Array[String]): Unit = {
    new Spark_Dataframe().schema();
    new Spark_Dataframe().schema1();
  }
}
