package com.hl.bigdata.spark.scala.sql

/**
 * @author huanglin
 * @date 2023/03/07 22:43
 */
object Spark_Parquet extends Spark_Base {
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    val peopleDf = spark.read.json(sc.wholeTextFiles("/user/spark/spark_json2.json").values);
    peopleDf.show();
    // 写入默认数据源格式文件
//    peopleDf.write.format("parquet").save("/user/spark/spark.parquet");
    val sqlDf = spark.sql("select * from parquet.`hdfs://s100:8020/user/spark/spark.parquet`");
    sqlDf.show();

    // 数据源格式的schema合并
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double");
    df1.write.parquet("hdfs://s100:8020/user/spark/spark_df2/key=1");
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple");
    df2.write.parquet("hdfs://s100:8020/user/spark/spark_df2/key=2");
    val df3 = spark.read.option("mergeSchema", "true").parquet("hdfs://s100:8020/user/spark/spark_df2");
    df3.printSchema();
    df3.show();
  }
}
