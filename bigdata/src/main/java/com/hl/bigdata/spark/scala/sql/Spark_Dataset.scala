package com.hl.bigdata.spark.scala.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author huanglin
 * @date 2023/03/01 21:58
 */
case class Point(label: String, x: Double, y: Double);
case class Category(id: Long, name: String);
class Spark_Dataset extends Spark_Base {
  //   需要导入隐式转换
  import spark.implicits._

  //通过createDataset（seq,list,rdd）
  def def1() : Unit = {

    // 通过seq创建Dataset
    val seqDs: Dataset[Int] = spark.createDataset(1 to 10);
    // 通过list创建Dataset
    val listDs: Dataset[(String, Int)] = spark.createDataset(List(("a", 1),("b", 2), ("c", 3)));
    // 通过rdd创建Dataset
    val rddDs: Dataset[(String, Int, Int)] = spark.createDataset(sc.parallelize(List(("a", 1, 33),("b", 2, 55), ("c", 3, 35))))

    seqDs.show();
    listDs.show();
    rddDs.show();
  }

  // 通过case class样例类创建一个seq、list、Array、RDD，再.toDS转化为Dataset
  def def2() : Unit = {

    // 通过Point的样例类创建一个seq，并将它转化为Datase
    val points : Dataset[Point] = Seq(Point("bar", 3.3, 5.5), Point("foo", 3.5, 5.3)).toDS();
    // 通过Category样例类
    val category : Dataset[Category] = Seq(Category(1, "bar"), Category(2, "foo")).toDS();
    // 进行join连接,注意这里需要传入三个“===”,这时一个方法
    points.join(category, points("label") === category("name")).show();

    // 通过Point的样例类创建一个List,并将它转化为Dataset
    val points1 : Dataset[Point] = List(Point("bar", 3.3, 5.5), Point("foo", 3.5, 5.3)).toDS();
    // 通过Category的样例类创建一个List，并将它转化为Dataset
    val category1 : Dataset[Category] = List(Category(1, "bar"), Category(2, "foo")).toDS();
    // 进行join连接,注意这里需要传入三个“===”,这时一个方法
    points1.join(category1, points1("label") === category1("name")).show();

    // 通过Point的样例类创建一个RDD，并将它转化为Dataset
    val points2 : Dataset[Point] = sc.parallelize(List(Point("bar", 3.3, 5.5), Point("foo", 3.5, 5.3))).toDS();
    // 通过Category的样例类创建一个RDD，并将它转化为Dataset
    val category2 : Dataset[Category] = sc.parallelize(List(Category(1, "bar"), Category(2, "foo"))).toDS();
    points2.join(category2, points2("label") === category2("name")).show();
  }

  // 先创建RDD，在把RDD和样例类进行关联,再.toDS转化为Dataset
  def def3() : Unit = {
    // 通过Point的数据创建一个RDD
    val pointRdd : RDD[(String, Double, Double)] = sc.parallelize(List(("bar", 3.3, 5.5), ("foo", 3.5, 5.3)));
    // 通过Category的数据创建一个RDD
    val categoryRdd : RDD[(Int, String)] = sc.parallelize(List((1, "bar"), (2, "foo")));
    // 两个RDD和样例类进行关联
    val pointDs : Dataset[Point] = pointRdd.map(x => Point(x._1, x._2, x._3)).toDS();
    val categoryDs : Dataset[Category] = categoryRdd.map(x => Category(x._1, x._2)).toDS();
    // join操作
    pointDs.join(categoryDs, pointDs("label") === categoryDs("name")).show();
  }
}

object Spark_Dataset {

  def main(args: Array[String]): Unit = {
    new Spark_Dataset().def1();
    new Spark_Dataset().def2();
    new Spark_Dataset().def3();
  }
}
