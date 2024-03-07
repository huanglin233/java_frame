package com.hl.bigdata.spark.scala.rdd

import com.hl.bigdata.spark.scala.customObj.CustomAccumulator

/**
 * 累加器
 *
 * @author huanglin
 * @date 2022/04/12 22:05:33
 */
class Rdd_Accumulator extends Rdd_BASE {

  def accumulator(): Unit = {
    val rdd = sc.textFile("file:/home/huanglin/spark_log/log.txt", 3);
    val blanklines = sc.longAccumulator;
    val rdd_map = rdd.flatMap(line => {
      if(line.length == 0) {
        println(line);
        blanklines.add(1);
      }
      line.split(",");
    })
    println(rdd_map.foreach(print));
    println(rdd_map.count());
    println(blanklines.value);
  }

  def custom_accumulator(): Unit = {
    val custom_accumulator  = new CustomAccumulator();
    sc.register(custom_accumulator);
    val sum = sc.parallelize(List("1", "2a", "3b", "4", "5", "6", "7c")).filter(item => {
      val parttern = """^-?(\d+)""";
      val flag = item.matches(parttern);
      if(!flag) {
        custom_accumulator.add(item);
      }
      flag
    }).map(_.toInt).reduce(_ + _);

    println("sum " + sum);
    println(custom_accumulator.value)
    sc.stop();
  }
}

object Rdd_Accumulator {

  def main(args: Array[String]): Unit = {
//    new Rdd_Accumulator().accumulator();
    new Rdd_Accumulator().custom_accumulator();
  }
}
