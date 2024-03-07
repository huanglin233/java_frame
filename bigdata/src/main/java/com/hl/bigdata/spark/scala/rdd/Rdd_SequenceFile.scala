package com.hl.bigdata.spark.scala.rdd

import org.apache.hadoop.io.{IntWritable, Text}

/**
 * @author huanglin
 * @date 2022/03/26 19:52:09
 */
class Rdd_SequenceFile extends Rdd_BASE {

  def sequenceFile(): Unit = {
    val rdd14 = sc.parallelize(List((1, "jack"), (3, "jim"), (3, "tom"), (2, "mary")));
    // 保存为序列文件
    val rm_rdd = rdd14.pipe("rm -rf /home/huanglin/spark_log/seqFile")
    println(rm_rdd.foreach(println));
    rdd14.saveAsSequenceFile("file:/home/huanglin/spark_log/seqFile");
    // 读取序列文件
    val readSeq_rdd = sc.sequenceFile("file:/home/huanglin/spark_log/seqFile", classOf[IntWritable], classOf[Text]);
    readSeq_rdd.map(item => {
      println(item._1, item._2)
    }).collect;
  }
}

object Rdd_SequenceFile {

  def main(args: Array[String]): Unit = {
    new Rdd_SequenceFile().sequenceFile();
  }
}
