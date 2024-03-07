package com.hl.bigdata.spark.scala.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
 * @author huanglin
 * @date 2023/03/04 20:38
 */
class Spark_UDAF extends UserDefinedAggregateFunction {

  // 聚合函数输入参数的数据类型
  override def inputSchema: StructType = StructType(StructField("inputColum", LongType) :: Nil) // :: 用于的是向队列的头部追加数据,产生新的列表

  // 聚合函数缓冲区中值的数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil) // Nil是一个空的List,定义为List[Nothing]
  }

  // 返回的值的数据类型
  override def dataType: DataType = DoubleType

  // 对于相同的输入是否一直返回相同的输出
  override def deterministic: Boolean = true

  // 初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 年龄的总额
    buffer(0) = 0L;
    // 用户数
    buffer(1) = 0L;
  }

  // 相同Execute间的数据合并(同一分区)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0);
      buffer(1) = buffer.getLong(1) + 1;
    }
  }

  // 不同Execute间的数据合并(不同分区)
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0);
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1);
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1);
}

object Spark_UDAF {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN);

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName(this.getClass.getName).getOrCreate();
    val sc: SparkContext = spark.sparkContext;
    sc.setLogLevel("WARN");
    import spark.implicits._
    spark.udf.register("myAverage", new Spark_UDAF());

    val df = spark.read.json(sc.wholeTextFiles("/user/spark/spark_json2.json").values);
    df.createOrReplaceTempView("people");
    df.show();
    val result = spark.sql("select myAverage(age) as average_age from people");
    result.show();
    spark.stop();
  }
}