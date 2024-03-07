package com.hl.bigdata.spark.scala.rdd

import org.apache.spark.rdd.JdbcRDD

import java.sql.DriverManager

/**
 * @author huanglin
 * @date 2022/04/09 14:54:57
 */
class Rdd_Mysql extends Rdd_BASE {

  def rdd_mysql(): Unit = {
    // 读mysql
    val rdd_mysql = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?useSSL=false", "root", "root");
      },
      "select * from t1 where id >= ? and id < ?",
      1,
      3,
      1,
       r => (r.getInt(1), r.getString(2))
    );
    println(rdd_mysql.count());
    println(rdd_mysql.foreach(println(_)));

    // 写入mysql
    val rdd_mysql_collect = sc.parallelize(List("hello4", "hello5", "hello6"));
    Class.forName("com.mysql.jdbc.Driver");
    val conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?useSSL=false", "root", "root");
    rdd_mysql_collect.collect.foreach(c => {
      val ps = conn.prepareStatement("insert into t1(msg) values(?)");
      ps.setString(1, c);
      ps.executeUpdate();
    })

    sc.stop();
  }
}

object Rdd_Mysql {

  def main(args: Array[String]): Unit = {
    new Rdd_Mysql().rdd_mysql();
  }
}
