package com.hl.bigdata.spark.scala.rdd

import com.hl.bigdata.spark.scala.customObj.CustomPartitioner
import org.apache.spark.{HashPartitioner, RangePartitioner}

/**
 * @author huanglin
 * @date 2022/03/26 18:19:46
 */
class Rdd_Partitions extends Rdd_BASE {

  def partitions(): Unit = {
    val rdd = sc.parallelize(Seq((1, 22), (1, 33), (2, 33), (3, 33), (4, 55)));
    println(rdd.partitions.length);
    // 降低分区
    val rdd4 = rdd.coalesce(1);
    println(rdd4.partitions.length);
    // 再分区，可增可减
    val rdd5 = rdd4.repartition(3);
    println(rdd5.partitions.length);
    val rdd6 = rdd5.map((_, 1));
    println(rdd6.partitions.length);


    println("<---------------------------------->");
    // hash分区
    rdd6.partitionBy(new HashPartitioner(2));
    println(rdd6.partitions.length);
    println(rdd6.partitioner);

    println("<---------------------------------->");
    val rdd15 = sc.makeRDD(Seq((1, "sql1"), (2, "sql2"), (3, "sql3"), (4, "sql4")));
    println(rdd15.partitions.length);
    println(rdd15.preferredLocations(rdd15.partitions(0)));
    println(rdd15.preferredLocations(rdd15.partitions(1)));
    println(rdd15.preferredLocations(rdd15.partitions(2)));
    println("<---------------------------------->");
    val rdd14 = sc.parallelize(List((1, "jack"), (3, "jim"), (3, "tom"), (2, "mary")));
    val customPar_rdd = rdd14.partitionBy(new CustomPartitioner());
    val groupKey_rdd = customPar_rdd.groupByKey();
    println(groupKey_rdd.foreach(println));
  }
}

object Rdd_Partitions {

  def main(args: Array[String]): Unit = {
    new Rdd_Partitions().partitions();
  }
}
