package com.hl.bigdata.spark.scala.rdd

import org.json4s.jackson.JsonMethods

/**
 * @author huanglin
 * @date 2022/04/09 13:03:54
 */
class Rdd_Json extends Rdd_BASE {

  def rdd_json() : Unit = {
    val cur_path = this.getClass.getResource("/");
    // 操作json文本需要导入 org.json4s相关的jar包
    import org.json4s._
    val json_text =  sc.textFile(cur_path + "spark_json.json");
    json_text.collect.foreach(println);
    println("------------------------")
    // 导入隐式函数
    implicit val f = org.json4s.DefaultFormats
    val json = new StringBuilder();
    json_text.collect.foreach(x => json.append(x.mkString));
    println(json.toString());
    var c = JsonMethods.parse(json.toString()).extract[List[Rdd_Person]];
    c.foreach(x => println(x.name + " " + x.age + " " + x.sex));
  }
}

object Rdd_Json {
  def main(args: Array[String]): Unit = {
    new Rdd_Json().rdd_json();
  }
}
